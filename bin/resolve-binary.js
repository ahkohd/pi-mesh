import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";

const require = createRequire(import.meta.url);

export function resolveBinary(name) {
  const packageName = platformPackageName();
  if (!packageName) {
    throw new Error(`unsupported platform for ${name}: ${process.platform} ${process.arch}`);
  }

  let packageJson;
  try {
    packageJson = require.resolve(`${packageName}/package.json`);
  } catch {
    throw new Error(`missing optional package ${packageName}; reinstall @ahkohd/pi-mesh`);
  }

  const binary = join(dirname(packageJson), "bin", executableName(name));
  if (!existsSync(binary)) {
    throw new Error(`missing bundled binary: ${binary}`);
  }

  return binary;
}

function platformPackageName() {
  if (process.platform === "darwin" && process.arch === "arm64") return "@ahkohd/pi-mesh-darwin-arm64";
  if (process.platform === "darwin" && process.arch === "x64") return "@ahkohd/pi-mesh-darwin-x64";
  if (process.platform === "linux" && process.arch === "arm64") return "@ahkohd/pi-mesh-linux-arm64-gnu";
  if (process.platform === "linux" && process.arch === "x64") return "@ahkohd/pi-mesh-linux-x64-gnu";
  return undefined;
}

function executableName(name) {
  return process.platform === "win32" ? `${name}.exe` : name;
}
