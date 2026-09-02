"""Snowflake execution engine.

Submits profiling and fingerprinting work to Snowflake compute through the
`snowpark-submit` Python API (`snowflake.snowpark_submit`), not its CLI. The
API is authenticated by an ordinary `snowflake.snowpark.Session`, so this
engine never shells out to a subprocess: no argv to quote, no stdout/stderr
to scrape for a workload name or a status line, and the PAT never touches a
process table.

Unlike the Yeedu and Databricks engines, which post a job specification to a
REST endpoint, Snowflake has no HTTP endpoint that submits a Spark workload
directly. `SnowparkSubmit` is a client-side orchestrator: it stages the
application files, provisions a Snowpark Connect server on a compute pool,
runs the script against it, and reports workload status through the same
session used to authenticate.

NOTE: this is Snowflake as a *compute platform*. It is unrelated to reading a
Snowflake database as a JDBC *source*, which happens whenever the payload
carries a Snowflake `derived_jdbc_url` and can run on any engine. The two are
independent and may be combined.

Only three fields are asked of the credential API — everything else a
session needs is resolved from the token itself, described below.

    snowflake_account       the account identifier, e.g. "myorg-myaccount"
    snowflake_token         the PAT
    snowflake_compute_pool  the pool the workload runs on

## Everything else comes from the token, not from configuration

A programmatic access token does not carry a username in a form the classic
connector login flow can use — authenticating with only a token and no
`user` fails server-side ("Programmatic access token is invalid"), because
Snowflake needs the login name to know whose token it is validating. But
Snowflake's REST SQL API resolves identity from the bearer token alone: a
`POST /api/v2/statements` request carrying only
`Authorization: Bearer <token>` and
`X-Snowflake-Authorization-Token-Type: PROGRAMMATIC_ACCESS_TOKEN` answers
`CURRENT_USER()` correctly with no username supplied anywhere. The same
request resolves warehouse, database, schema, and role in one round trip.

`_resolve_identity()` uses exactly this to fill in `user`, `warehouse`,
`database`, `schema`, and `role` before a `Session` is ever created — none of
these are asked for, guessed, or read from the environment. This is not a
convenience; it is the only way to know them at all, since the classic
connector session used by `SnowparkSubmit` needs `user` before it can open a
session to ask Snowflake anything.

Nothing here is defaulted except schema, which falls back to PUBLIC if the
resolved value is null — see `_resolve_identity`'s docstring for why that one
case is different.

The UI's field names are accepted alongside the `snowflake_`-prefixed ones —
`account_identifier`, `pat_token` — since the linked-account form writes
those. The account identifier arrives as a full URL and is trimmed back to
the bare identifier, which the session wants, and the host is rebuilt from
it.

NOTE: Snowflake refuses PAT authentication entirely unless a network policy
is attached to the user or account, failing with "Network policy is
required" or "Incoming request with IP/Token ... is not allowed to access
Snowflake." That is account setup the deployment must do; no code here can
substitute for it.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import sys
import subprocess
import tempfile
import zipfile
from typing import Dict, List, Optional

import requests

from engines import config as cfg
from engines.base import ComputeEngine, JobState

logger = logging.getLogger(__name__)

#: Appended to the account identifier to form the session host. Snowflake
#: accounts are always reachable at this domain, so the host is derived
#: rather than asked for: the UI collects an account identifier, not a URL.
SNOWFLAKE_DOMAIN = ".snowflakecomputing.com"

#: Used when the token's own session reports no current schema. Snowflake
#: creates PUBLIC in every database, so it exists unless explicitly dropped.
DEFAULT_SCHEMA = "PUBLIC"

# Snowpark Connect workload states → normalised JobState. workload_status
# from StatusInfo is the primary signal; the extra aliases guard against
# Snowflake reporting the underlying service status under different wording,
# which this project has no way to enumerate exhaustively from outside.
_STATE_MAP = {
    "DONE": JobState.DONE,
    "SUCCEEDED": JobState.DONE,
    "FAILED": JobState.FAILED,
    "INTERNAL_ERROR": JobState.FAILED,
    "CANCELLED": JobState.STOPPED,
    "CANCELED": JobState.STOPPED,
    "RUNNING": JobState.RUNNING,
    "PENDING": JobState.RUNNING,
    "SUSPENDED": JobState.RUNNING,
}

#: src/ ships as one zip on py_files. The name matches the other engines so
#: profiling.py's import fallback resolves the same way everywhere.
SRC_BUNDLE_NAME = "forgeai_src.zip"

# The Snowflake workload container's interpreter, established by running a
# probe job against it. Wheels must match this, not the submitting host's:
# a wheel for the wrong ABI fails at import exactly as a missing one does.
WORKLOAD_PYTHON = "3.11"
WORKLOAD_ABI = "cp311"
WORKLOAD_PLATFORM = "manylinux2014_x86_64"

#: Packages the workload image does not carry, from the same probe. pandas,
#: numpy, requests, pydantic, boto3, cryptography and PyJWT are already there
#: and are deliberately not re-shipped.
WORKLOAD_MISSING_PACKAGES = (
    "psycopg2-binary", "python-dotenv", "pyhocon", "sqlalchemy", "redis",
    "tqdm", "langchain-core", "langchain-openai", "langchain-anthropic",
)

#: Timeout for the identity-resolution query and the underlying REST call
#: SnowparkSubmit's status/kill operations make. Both are single-row,
#: metadata-only round trips, not workload-shaped work.
_IDENTITY_QUERY_TIMEOUT = 30


class SnowflakeConfigError(Exception):
    """Raised when the Snowflake credentials or compute settings are unusable."""


class SnowflakeEngine(ComputeEngine):
    """Execute profiling/fingerprinting jobs on Snowflake compute."""

    name = "snowflake"

    def __init__(self, payload: dict, forgeai_home: str):
        super().__init__(payload, forgeai_home)
        self._conn: Optional[Dict[str, str]] = None
        self._snowpark_session = None
        self._client = None

    # ═════════════════════════════════════════════════════════════════════════
    # Public interface
    # ═════════════════════════════════════════════════════════════════════════

    def run_profiling(self, cluster_name_override: str = None) -> int:
        """Run profiling.py on Snowflake compute via Snowpark Submit."""
        return self._submit_and_monitor(
            description="Python profiling",
            job_name=self.profiling_job_name,
            entrypoint="profiling.py",
            arguments=self.build_profiling_args(),
            warehouse_override=cluster_name_override,
            upload_src=True,
        )

    def run_scala_job(self, scala_payload: dict, cluster_name_override: str = None,
                      class_name: str = cfg.SCALA_MAIN_CLASS) -> int:
        """Run the fingerprinting JAR on Snowflake compute."""
        conf_file = f"{self.forgeai_home}/conf/forgeai_spark_conf.sh"
        version = self.get_conf_var("forgeai_master_data_manager", conf_file)
        jar_name = f"forgeai-master-data-manager-assembly-{version}.jar"
        encoded = base64.b64encode(
            json.dumps(scala_payload).encode("utf-8")).decode("utf-8")

        return self._submit_and_monitor(
            description="Scala fingerprinting",
            job_name=self.scala_job_name(class_name),
            entrypoint=jar_name,
            arguments=[encoded],
            warehouse_override=cluster_name_override,
            upload_src=False,
            main_class=class_name,
        )

    def stop(self, run_id) -> bool:
        """Terminate a running Snowpark Connect workload."""
        try:
            self._connect()
            info = self._client_handle().kill(
                workload_name=str(run_id), compute_pool=self._compute_pool())
            if info.error:
                logger.error("Failed to terminate Snowflake workload %s: %s",
                             run_id, info.error)
                return False
            logger.info("Snowflake workload %s terminated", run_id)
            return True
        except Exception as e:
            logger.error("Failed to terminate Snowflake workload %s: %s", run_id, e)
            return False

    # ═════════════════════════════════════════════════════════════════════════
    # Shared job lifecycle
    # ═════════════════════════════════════════════════════════════════════════

    def _submit_and_monitor(self, description: str, job_name: str, entrypoint: str,
                            arguments: List[str], warehouse_override: str,
                            upload_src: bool, main_class: str = None) -> int:
        """Submit the workload through the Snowpark Submit API and monitor it.

        Staging is left to SnowparkSubmit, which uploads the entrypoint and
        everything named by py_files/files/jars itself. Doing our own PUTs
        first would upload the same files twice.
        """
        try:
            self._connect(warehouse_override)

            logger.info("=" * 60)
            logger.info("SNOWFLAKE %s: compute_pool=%s, job=%s",
                        description.upper(), self._compute_pool(), job_name)
            logger.info("=" * 60)

            if self.is_job_stopped():
                logger.info("Stop detected before job submission — aborting")
                return cfg.ExitCode.STOPPED

            workload = self._submit_job(job_name, entrypoint, arguments, main_class)
            self.save_spark_run_id_safe(workload)

            return self.monitor(workload,
                                poll_interval=cfg.Timeouts.POLL_INTERVAL_SLOW,
                                description=description)

        except Exception as e:
            logger.error("Snowflake %s failed: %s", description, e)
            return cfg.ExitCode.FAILED

    def _submit_job(self, job_name: str, entrypoint: str,
                    arguments: List[str], main_class: str = None) -> str:
        """Submit through the Snowpark Submit API and return the assigned
        workload name.

        `application_args` is a real list here, not shell-joined text — the
        CLI form of this engine needed to quote every argument against a
        remote shell that re-evaluated the command line; the API form has no
        shell in the path at all, so a JDBC URL containing `&` or `?` needs
        no special handling.
        """
        from snowflake.snowpark_submit import WorkloadConfig

        workload_config = WorkloadConfig(
            file=self._entrypoint_path(entrypoint, main_class),
            compute_pool=self._compute_pool(),
            workload_name=self._workload_base_name(job_name),
            main_class=main_class,
            application_args=[str(a) for a in arguments if a is not None],
            comment=job_name,
            **self._file_staging_kwargs(main_class),
        )

        logger.info("Submitting Snowflake workload %s", job_name)
        result = self._client_handle().submit(
            workload_config, wait_for_completion=False)

        if result.error:
            raise RuntimeError(f"Snowpark Submit failed: {result.error}")
        if not result.workload_name:
            raise RuntimeError(
                "Snowpark Submit did not report a workload name; cannot "
                f"track the job. {result.error or ''}".strip())

        logger.info("Snowflake workload name: %s", result.workload_name)
        return result.workload_name

    def _entrypoint_path(self, entrypoint: str, main_class: Optional[str]) -> str:
        """Local path to the file SnowparkSubmit uploads and runs."""
        if main_class:
            return f"{self.forgeai_home}/jars/{entrypoint}"
        return f"{self.forgeai_home}/src/{entrypoint}"

    def _file_staging_kwargs(self, main_class: Optional[str]) -> Dict[str, object]:
        """WorkloadConfig kwargs for everything besides the entrypoint itself.

        Kept as one comma-separated string per field, matching the shape
        WorkloadConfig documents (py_files/files/jars are "comma-separated
        list of ..." strings, not Python lists) — only application_args is a
        real list.
        """
        kwargs: Dict[str, object] = {}

        wheels = self._dependency_wheels()
        staged = list(self._runtime_files()) + wheels
        if staged:
            kwargs["files"] = ",".join(staged)
        if wheels:
            # Shipped via `files`, not `wheel_files`: left to its own
            # handling, pip resolves each wheel's dependencies against PyPI
            # and fails on the first one it cannot reach, even though every
            # dependency was uploaded alongside it. init_script installs
            # them offline instead.
            kwargs["init_script"] = self._write_wheel_install_script()

        conf = self._source_credential_conf()
        if conf:
            kwargs["conf"] = conf

        if main_class:
            # Scala/JAR workload: the assembly is the application. No
            # py_files/jars of its own beyond what _runtime_files staged.
            return kwargs

        # Python workload. src/ ships as a zip on the PYTHONPATH so
        # profiling.py can import its siblings, mirroring the other engines.
        src_bundle = self._python_dependencies()
        if src_bundle:
            kwargs["py_files"] = src_bundle
        jars = self._jar_list()
        if jars:
            kwargs["jars"] = jars
        return kwargs

    def _workload_base_name(self, job_name: str) -> str:
        """Sanitise a job name into a valid unquoted Snowflake identifier.

        Snowpark Submit rejects anything else, and our job names carry
        hyphens and dots that would fail that check.
        """
        cleaned = re.sub(r"[^A-Za-z0-9_$]", "_", str(job_name))
        if not cleaned or not re.match(r"[A-Za-z_]", cleaned[0]):
            cleaned = f"forgeai_{cleaned}"
        return cleaned[:180]

    def _python_dependencies(self) -> Optional[str]:
        """Build the src bundle and return it as a py_files value.

        profiling.py imports its siblings — `metadata_store`, `semantics` and
        the rest — so shipping the entrypoint alone leaves the workload
        failing at import. The modules are zipped flat, matching the
        bare-name fallback imports profiling.py falls back to when `src.` is
        not a package on the remote side.

        Built per submission rather than looked up on disk: there is no
        build step that produces a src.zip, so searching for one silently
        found nothing and dropped py_files from the workload entirely.
        """
        src_dir = os.path.join(self.forgeai_home, "src")
        if not os.path.isdir(src_dir):
            logger.warning("No src/ directory at %s; submitting without "
                           "py_files", src_dir)
            return None

        bundle_dir = tempfile.mkdtemp(prefix=f"forgeai_sf_{self.job_id}_")
        bundle = os.path.join(bundle_dir, SRC_BUNDLE_NAME)
        with zipfile.ZipFile(bundle, "w", zipfile.ZIP_DEFLATED) as archive:
            for filename in sorted(os.listdir(src_dir)):
                path = os.path.join(src_dir, filename)
                if os.path.isfile(path) and filename.endswith(".py"):
                    archive.write(path, filename)

        logger.debug("Bundled src/ for py_files: %s", bundle)
        return bundle

    def _jar_list(self) -> Optional[str]:
        """Comma-separated jars value from the local jars directory."""
        jar_dir = f"{self.forgeai_home}/jars"
        if not os.path.isdir(jar_dir):
            return None
        jars = [os.path.join(jar_dir, f)
                for f in sorted(os.listdir(jar_dir)) if f.endswith(".jar")]
        return ",".join(jars) if jars else None

    def _client_handle(self):
        """The cached SnowparkSubmit client for the current session."""
        if self._client is None:
            from snowflake.snowpark_submit import SnowparkSubmit
            self._client = SnowparkSubmit(self._snowpark_session)
        return self._client

    def fetch_state(self, run_id) -> Optional[str]:
        """Map the workload's reported status onto a normalised JobState."""
        info = self._client_handle().status(
            workload_name=str(run_id), compute_pool=self._compute_pool())

        status = (info.workload_status or "").upper()
        # workload_status can read DONE even when the application itself
        # exited non-zero — the *service* completed, but the script failed.
        # Treating that as done would hide a real failure from the retry and
        # reporting logic that reads fetch_state's return value.
        if status == "DONE" and info.job_exit_code not in (None, 0):
            status = "FAILED"

        if not status:
            # Nothing reported yet: the workload may still be provisioning.
            # Returning None makes the shared monitor poll again rather than
            # treating an unknown/empty status as terminal.
            return None

        state = _STATE_MAP.get(status)
        if state:
            return state
        logger.debug("Unrecognised workload status %r for %s", status, run_id)
        return None

    def fetch_stderr(self, run_id) -> str:
        """Application logs for a failed workload, used for OOM detection."""
        try:
            info = self._client_handle().status(
                workload_name=str(run_id), compute_pool=self._compute_pool(),
                display_logs=True)
            lines = list(info.logs or [])
            if info.error:
                lines.append(info.error)
            return "\n".join(lines)
        except Exception as e:
            logger.debug("Could not fetch logs for workload %s: %s", run_id, e)
            return ""

    def save_spark_run_id_safe(self, handle: str):
        """Persist the statement handle when it is numeric.

        checkpoint_status.spark_run_id is an integer column, but Snowflake
        workload names are not, so they are logged instead of stored.
        """
        try:
            self.save_spark_run_id(int(handle))
        except (TypeError, ValueError):
            logger.info("Snowflake workload name %s is not numeric; "
                        "not persisting to spark_run_id", handle)

    # ═════════════════════════════════════════════════════════════════════════
    # Credentials and compute resolution
    # ═════════════════════════════════════════════════════════════════════════

    def _connect(self, warehouse_override: str = None):
        """Resolve credentials and open the Snowpark session.

        Only account, token, and compute_pool come from the credential API.
        Everything else a session needs — user, warehouse, database, schema,
        role — is resolved from the token itself by `_resolve_identity`.
        """
        if self._snowpark_session is not None:
            if warehouse_override:
                self._conn["warehouse"] = warehouse_override
                self._snowpark_session = None  # a new warehouse needs a new session
                self._client = None
            else:
                return

        self._conn = {}

        compute_cfg = self.get_compute_details()
        # The linked-account form the UI writes uses its own field names, so
        # each setting is looked up under those as well as the snowflake_
        # prefixed ones. Order is deliberate: a `snowflake_`-prefixed value
        # wins, since it is the explicit form. The account arrives as
        # `snowflake_url` in the linked-account config actually observed from
        # the credential API — `account_identifier` is kept too since it's
        # the name the docstring's own description of the UI form uses, and
        # costs nothing to also accept.
        aliases = {"token": ("pat_token",),
                  "account": ("snowflake_url", "account_identifier")}
        for key in ("token", "account", "compute_pool"):
            value = compute_cfg.get(f"snowflake_{key}") or compute_cfg.get(key)
            for alias in aliases.get(key, ()):
                if value:
                    break
                value = compute_cfg.get(alias)
            if value:
                self._conn[key] = value

        # The UI collects a URL — https://<account>.snowflakecomputing.com —
        # not the bare account identifier the session wants, and the host is
        # derived by putting the domain back, so normalising here keeps one
        # field serving both. Scheme and domain are stripped independently:
        # a value with only one of them (a bare host, or an identifier that
        # already has no scheme) must still come out right.
        account = self._conn.get("account")
        if account:
            account = re.sub(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", "", account)
            if account.endswith(SNOWFLAKE_DOMAIN):
                account = account[:-len(SNOWFLAKE_DOMAIN)]
            self._conn["account"] = account

        # The credential API stores tokens base64-encoded, matching how the
        # Yeedu token arrives. Decode only if it actually is base64: a PAT
        # supplied in the clear should pass through untouched rather than be
        # mangled into bytes that fail authentication with no clue why.
        token = self._conn.get("token")
        if token:
            self._conn["token"] = self._maybe_b64decode(token)

        self._require_auth_fields()
        self._resolve_identity()

        if warehouse_override:
            self._conn["warehouse"] = warehouse_override

        logger.info(
            "Snowflake compute resolved: token=%s account=%s pool=%s "
            "user=%s warehouse=%s database=%s schema=%s",
            "yes" if self._conn.get("token") else "MISSING",
            self._conn.get("account") or "MISSING",
            self._conn.get("compute_pool") or "MISSING",
            self._conn.get("user") or "MISSING",
            self._conn.get("warehouse") or "MISSING",
            self._conn.get("database") or "-",
            self._conn.get("schema") or "-")

        self._snowpark_session = self._open_session()

    def _require_auth_fields(self) -> None:
        """The three fields nothing but the credential API can supply.

        There is no fallback. A missing field is reported here by name,
        rather than the engine reaching for some other source and
        authenticating as whoever that describes.
        """
        missing = [name for name, key in
                   (("snowflake_token", "token"),
                    ("snowflake_account", "account"),
                    ("snowflake_compute_pool", "compute_pool"))
                   if not self._conn.get(key)]
        if missing:
            raise SnowflakeConfigError(
                f"Snowflake credentials incomplete: the credential API must "
                f"return {', '.join(missing)}. None of these can be inferred "
                f"— a guessed account or compute pool authenticates or runs "
                f"against something the account may not have, and reports "
                f"the mistake as a Snowflake error rather than as the "
                f"misconfiguration it is.")

    def _resolve_identity(self) -> None:
        """Resolve user, warehouse, database, schema, and role from the PAT.

        A PAT does not carry a login name the classic connector session can
        use — authenticating with a token and no user fails server-side with
        "Programmatic access token is invalid", because Snowflake needs the
        login name to know whose token it is checking. But the REST SQL API
        resolves identity from the bearer token alone: this queries
        CURRENT_USER() and friends with only `Authorization: Bearer <token>`
        and no username anywhere in the request, then uses the answer to
        open the classic session SnowparkSubmit needs.

        Schema is the one value with a fallback: a user can have no default
        schema at all (CURRENT_SCHEMA() returns NULL), and Snowpark Connect
        then dies partway through staging its own files with "This session
        does not have a current schema." PUBLIC exists in every database
        Snowflake creates, so it is the safe answer — unlike a guessed
        warehouse or compute pool, naming the wrong schema cannot run a
        workload somewhere unintended.

        Not best-effort: this is the only source for `user`, so a failure
        here is a failure to authenticate at all, not a gap something else
        can paper over.
        """
        account = self._conn["account"]
        host = f"{account}{SNOWFLAKE_DOMAIN}"
        url = f"https://{host}/api/v2/statements"
        headers = {
            "Authorization": f"Bearer {self._conn['token']}",
            "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        body = {
            "statement": ("SELECT CURRENT_USER(), CURRENT_WAREHOUSE(), "
                         "CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE()"),
            "timeout": _IDENTITY_QUERY_TIMEOUT,
        }

        try:
            response = requests.post(url, headers=headers, json=body,
                                     timeout=_IDENTITY_QUERY_TIMEOUT)
            response.raise_for_status()
            row = response.json()["data"][0]
        except Exception as e:
            raise SnowflakeConfigError(
                f"Could not resolve the Snowflake session from the PAT: {e}. "
                f"Check that the token is valid and that a network policy "
                f"allows this host to reach {host}.") from e

        user, warehouse, database, schema, role = row
        if not user:
            raise SnowflakeConfigError(
                "CURRENT_USER() resolved to nothing for this token; cannot "
                "open a session without a login name.")

        self._conn["user"] = user
        if warehouse:
            self._conn["warehouse"] = warehouse
        if database:
            self._conn["database"] = database
        self._conn["schema"] = schema or DEFAULT_SCHEMA
        if role:
            self._conn["role"] = role

    def _open_session(self):
        """Open the Snowpark session SnowparkSubmit authenticates through.

        Warehouse must be passed here, not left to a session default:
        Snowpark Connect does not activate the user's own default warehouse
        for the workload, and the job dies mid-run with "No active warehouse
        selected in the current session" even though the status banner shows
        one. Passing it explicitly — resolved from the token, not asked for
        — is what makes that default actually take effect.
        """
        from snowflake.snowpark import Session

        if not self._conn.get("warehouse"):
            raise SnowflakeConfigError(
                "No warehouse resolved from the Snowflake session. Snowpark "
                "Connect requires one explicitly and does not fall back to "
                "the user's own default; this session's token has none set, "
                "so a warehouse override must be supplied.")

        config = {
            "account": self._conn["account"],
            "host": f"{self._conn['account']}{SNOWFLAKE_DOMAIN}",
            "user": self._conn["user"],
            "password": self._conn["token"],
            "warehouse": self._conn["warehouse"],
            "schema": self._conn["schema"],
        }
        if self._conn.get("database"):
            config["database"] = self._conn["database"]
        if self._conn.get("role"):
            config["role"] = self._conn["role"]

        return Session.builder.configs(config).create()

    @staticmethod
    def _maybe_b64decode(value: str) -> str:
        """Decode a base64 token, or return it unchanged if it is not encoded.

        A Snowflake PAT is itself a dot-separated JWT, which is valid base64
        alphabet in each segment but not valid base64 as a whole — so a
        plain decode attempt fails cleanly and the original is kept.
        """
        try:
            decoded = base64.b64decode(value, validate=True).decode("utf-8")
        except Exception:
            return value
        return decoded if decoded.strip() else value

    def _compute_pool(self) -> str:
        """Compute pool the Snowpark Connect workload runs on.

        No default and not resolvable from the token — a compute pool is a
        deployment resource choice, not session state. A guessed pool name
        is not a smaller failure than a missing one — it submits against
        something the account may not have, or worse may have and not
        intend for this workload, and reports the mistake as a Snowflake
        error rather than as the misconfiguration it is.
        """
        pool = (self._conn or {}).get("compute_pool")
        if not pool:
            raise SnowflakeConfigError(
                "No Snowflake compute pool configured. Return "
                "snowflake_compute_pool from the credential API — it names "
                "the pool the workload runs on and cannot be inferred.")
        return pool

    def _dependency_wheels(self) -> List[str]:
        """Wheels for the packages Snowflake's workload image does not carry.

        Probed against a live workload: the image already provides pandas,
        numpy, requests, pydantic, boto3, cryptography and PyJWT, so only the
        remainder is shipped. Downloaded for the workload's interpreter
        (cp311, manylinux x86_64) rather than the submitting host's, since a
        wheel built for the wrong ABI fails at import with the same
        ModuleNotFoundError it was meant to prevent.

        Cached under the wheel directory: the download is ~26MB and does not
        change between jobs. Returned as a list here (unlike the other file
        lists) because it is merged into the same `files` value as the
        runtime config before being joined into one comma-separated string.
        """
        wheel_dir = os.getenv("SNOWFLAKE_WHEEL_DIR",
                              os.path.join(self.forgeai_home, "wheels"))
        if os.path.isdir(wheel_dir):
            wheels = sorted(os.path.join(wheel_dir, f)
                            for f in os.listdir(wheel_dir) if f.endswith(".whl"))
            if wheels:
                logger.info("Shipping %d dependency wheel(s) from %s",
                            len(wheels), wheel_dir)
                return wheels

        # Fetched on first use rather than baked into the image: the set is
        # ~26MB and only the Snowflake execution mode needs it, so paying for
        # it in every image — including deployments that never run Snowflake —
        # is the worse trade. Cached in the directory afterwards, so the cost
        # is one download per container.
        logger.info("No dependency wheels cached in %s; downloading", wheel_dir)
        return self._download_wheels(wheel_dir)

    def _download_wheels(self, wheel_dir: str) -> List[str]:
        """Fetch the workload's missing dependencies as wheels.

        Targets the workload's interpreter rather than this host's — the two
        differ, and a wheel built for the wrong ABI fails at import.
        """
        try:
            os.makedirs(wheel_dir, exist_ok=True)
        except OSError as e:
            logger.error("Cannot create %s for wheels: %s", wheel_dir, e)
            return []

        result = subprocess.run(  # noqa: S603 - argv list, no shell
            [sys.executable, "-m", "pip", "download", "--no-cache-dir",
             "--python-version", WORKLOAD_PYTHON, "--implementation", "cp",
             "--abi", WORKLOAD_ABI, "--platform", WORKLOAD_PLATFORM,
             "--only-binary=:all:", "-d", wheel_dir, *WORKLOAD_MISSING_PACKAGES],
            capture_output=True, text=True, timeout=600)

        if result.returncode != 0:
            logger.error("Could not download dependency wheels: %s",
                         (result.stderr or result.stdout or "")[-2000:])
            return []

        wheels = sorted(os.path.join(wheel_dir, f)
                        for f in os.listdir(wheel_dir) if f.endswith(".whl"))
        logger.info("Downloaded %d dependency wheel(s) to %s",
                    len(wheels), wheel_dir)
        return wheels

    def _write_wheel_install_script(self) -> str:
        """A shell script that installs the shipped wheels offline.

        --no-index is the point: without it pip contacts PyPI to resolve each
        wheel's dependencies and fails on the first unreachable one, even
        though every dependency was uploaded alongside it. --no-deps stops it
        walking the graph at all, which is safe because the downloaded set is
        already complete.
        """
        script_dir = tempfile.mkdtemp(prefix=f"forgeai_sf_init_{self.job_id}_")
        path = os.path.join(script_dir, "install_wheels.sh")
        with open(path, "w") as f:
            f.write(
                "#!/bin/bash\n"
                "set -e\n"
                "# Files land flat in /app on the workload node.\n"
                "python3 -m pip install --no-deps --no-index /app/*.whl\n"
            )
        os.chmod(path, 0o755)
        return path

    def _source_credential_conf(self) -> Dict[str, str]:
        """S3 credentials as Spark configuration, resolved before submitting.

        profiling.py fetches these from the credential API itself, but that
        API lives on the ForgeAI network and a Snowflake workload cannot
        reach it — the call fails with "Max retries exceeded", the keys stay
        None, and the bucket answers 403 for what is then an anonymous read.

        Resolving here works because the orchestrator does have that access.
        `spark.hadoop.fs.s3a.*` is where Spark's S3 connector looks anyway, so
        the reader picks them up without profiling.py knowing they arrived by
        a different route — and its own fetch still runs, so nothing
        regresses on the platforms where it succeeds.

        Only S3 for now: JDBC credentials travel inside the connection URL,
        which is built elsewhere, and Snowflake sources need the container's
        OAuth token rather than anything passed in.
        """
        if self.source_type != "s3":
            return {}

        creds = self.get_source_credentials()
        access_key = creds.get("access_key")
        secret_key = creds.get("secret_key")
        if not (access_key and secret_key):
            logger.warning(
                "No S3 credentials resolved; the workload will read anonymously "
                "and a private bucket will answer 403.")
            return {}

        return {
            "spark.hadoop.fs.s3a.access.key": access_key,
            "spark.hadoop.fs.s3a.secret.key": secret_key,
        }

    def _runtime_files(self) -> List[str]:
        """Config and key the job reads at runtime, for `files`.

        Snowpark Submit places anything named here into the workload node,
        so the upload is its job — but the files still have to be named, or
        profiling.py starts without its configuration.
        """
        config_file = f"{cfg.forgeai_env()}_application.conf"
        candidates = (f"{self.forgeai_home}/conf/{config_file}",
                      f"{self.forgeai_home}/conf/privateKey")
        return [p for p in candidates if os.path.isfile(p)]
