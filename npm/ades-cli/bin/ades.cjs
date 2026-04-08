#!/usr/bin/env node

const childProcess = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const packageJson = JSON.parse(
  fs.readFileSync(path.join(__dirname, "..", "package.json"), "utf8"),
);

const NPM_PACKAGE_NAME = packageJson.name;
const WRAPPER_VERSION = packageJson.version;
const COMMAND_NAME = "ades";
const RUNTIME_DIR_ENV = "ADES_NPM_RUNTIME_DIR";
const PYTHON_BIN_ENV = "ADES_PYTHON_BIN";
const PACKAGE_SPEC_ENV = "ADES_PYTHON_PACKAGE_SPEC";
const PIP_INSTALL_ARGS_ENV = "ADES_NPM_PIP_INSTALL_ARGS";
const MARKER_FILENAME = ".ades-python-package.json";
const INFO_FLAG = "--npm-bootstrap-info";

function main() {
  const args = process.argv.slice(2);
  const runtimeDir = resolveRuntimeDir();
  const info = buildBootstrapInfo(runtimeDir);
  if (args.includes(INFO_FLAG)) {
    process.stdout.write(`${JSON.stringify(info, null, 2)}\n`);
    return;
  }

  const pythonBin = resolvePythonBinary();
  ensureRuntimeInstalled(info, pythonBin);
  const runtimeCommand = resolveRuntimeCommand(runtimeDir);
  const execution = childProcess.spawnSync(runtimeCommand, args, {
    stdio: "inherit",
  });
  if (execution.error) {
    fail(execution.error.message);
  }
  process.exit(execution.status === null ? 1 : execution.status);
}

function buildBootstrapInfo(runtimeDir) {
  return {
    npmPackage: NPM_PACKAGE_NAME,
    command: COMMAND_NAME,
    wrapperVersion: WRAPPER_VERSION,
    pythonPackageSpec: resolvePythonPackageSpec(),
    runtimeDir,
    pythonBinEnv: PYTHON_BIN_ENV,
    runtimeDirEnv: RUNTIME_DIR_ENV,
    packageSpecEnv: PACKAGE_SPEC_ENV,
    pipInstallArgsEnv: PIP_INSTALL_ARGS_ENV,
  };
}

function resolvePythonPackageSpec() {
  return process.env[PACKAGE_SPEC_ENV] || `ades==${WRAPPER_VERSION}`;
}

function resolveRuntimeDir() {
  if (process.env[RUNTIME_DIR_ENV]) {
    return path.resolve(process.env[RUNTIME_DIR_ENV]);
  }
  if (process.platform === "win32") {
    const baseDir = process.env.LOCALAPPDATA
      ? process.env.LOCALAPPDATA
      : path.join(os.homedir(), "AppData", "Local");
    return path.join(baseDir, "ades", "npm-runtime", WRAPPER_VERSION);
  }
  if (process.platform === "darwin") {
    return path.join(
      os.homedir(),
      "Library",
      "Application Support",
      "ades",
      "npm-runtime",
      WRAPPER_VERSION,
    );
  }
  const xdgDataHome = process.env.XDG_DATA_HOME;
  const baseDir = xdgDataHome
    ? xdgDataHome
    : path.join(os.homedir(), ".local", "share");
  return path.join(baseDir, "ades", "npm-runtime", WRAPPER_VERSION);
}

function resolvePythonBinary() {
  if (process.env[PYTHON_BIN_ENV]) {
    const explicitPath = path.resolve(process.env[PYTHON_BIN_ENV]);
    assertExecutable(explicitPath, `Python executable not found: ${explicitPath}`);
    return explicitPath;
  }
  for (const candidate of ["python3", "python"]) {
    const probe = childProcess.spawnSync(candidate, ["--version"], {
      stdio: "ignore",
    });
    if (probe.status === 0) {
      return candidate;
    }
  }
  fail(
    `No usable Python interpreter found. Set ${PYTHON_BIN_ENV} to an explicit executable.`,
  );
}

function ensureRuntimeInstalled(info, pythonBin) {
  fs.mkdirSync(path.dirname(info.runtimeDir), { recursive: true });
  if (!runtimePythonExists(info.runtimeDir)) {
    runChecked(pythonBin, ["-m", "venv", info.runtimeDir], {
      stage: "create_venv",
    });
  }
  const runtimePython = resolveRuntimePython(info.runtimeDir);
  assertExecutable(
    runtimePython,
    `Bootstrap runtime Python is missing after venv creation: ${runtimePython}`,
  );

  const markerPath = path.join(info.runtimeDir, MARKER_FILENAME);
  const installedMarker = readMarker(markerPath);
  const runtimeCommand = resolveRuntimeCommand(info.runtimeDir);
  if (
    !installedMarker ||
    installedMarker.pythonPackageSpec !== info.pythonPackageSpec ||
    !fs.existsSync(runtimeCommand)
  ) {
    const pipArgs = splitArgs(process.env[PIP_INSTALL_ARGS_ENV] || "");
    runChecked(
      runtimePython,
      [
        "-m",
        "pip",
        "install",
        "--upgrade",
        ...pipArgs,
        info.pythonPackageSpec,
      ],
      { stage: "pip_install" },
    );
    fs.writeFileSync(
      markerPath,
      JSON.stringify(
        {
          npmPackage: info.npmPackage,
          wrapperVersion: info.wrapperVersion,
          pythonPackageSpec: info.pythonPackageSpec,
        },
        null,
        2,
      ) + "\n",
      "utf8",
    );
  }

  const finalCommand = resolveRuntimeCommand(info.runtimeDir);
  assertExecutable(
    finalCommand,
    `Bootstrap runtime command is missing after install: ${finalCommand}`,
  );
}

function splitArgs(value) {
  const trimmed = value.trim();
  if (!trimmed) {
    return [];
  }
  return trimmed.split(/\s+/u);
}

function runtimePythonExists(runtimeDir) {
  return fs.existsSync(resolveRuntimePython(runtimeDir));
}

function resolveRuntimePython(runtimeDir) {
  if (process.platform === "win32") {
    return path.join(runtimeDir, "Scripts", "python.exe");
  }
  return path.join(runtimeDir, "bin", "python");
}

function resolveRuntimeCommand(runtimeDir) {
  const candidates =
    process.platform === "win32"
      ? [
          path.join(runtimeDir, "Scripts", "ades.exe"),
          path.join(runtimeDir, "Scripts", "ades.cmd"),
          path.join(runtimeDir, "Scripts", "ades"),
        ]
      : [path.join(runtimeDir, "bin", "ades")];
  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }
  return candidates[0];
}

function readMarker(markerPath) {
  if (!fs.existsSync(markerPath)) {
    return null;
  }
  try {
    return JSON.parse(fs.readFileSync(markerPath, "utf8"));
  } catch {
    return null;
  }
}

function assertExecutable(targetPath, message) {
  if (!targetPath || !fs.existsSync(targetPath)) {
    fail(message || `Required executable not found: ${targetPath}`);
  }
}

function runChecked(command, args, metadata) {
  const execution = childProcess.spawnSync(command, args, {
    stdio: "inherit",
  });
  if (execution.error) {
    fail(
      `Bootstrap step failed (${metadata.stage}): ${execution.error.message}`,
    );
  }
  if (execution.status !== 0) {
    fail(
      `Bootstrap step failed (${metadata.stage}) with exit code ${execution.status}.`,
    );
  }
}

function fail(message) {
  process.stderr.write(`${message}\n`);
  process.exit(1);
}

main();
