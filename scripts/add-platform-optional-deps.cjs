#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");

const PLATFORM_DEPENDENCIES = [
  "@ahkohd/pi-mesh-darwin-arm64",
  "@ahkohd/pi-mesh-darwin-x64",
  "@ahkohd/pi-mesh-linux-arm64-gnu",
  "@ahkohd/pi-mesh-linux-x64-gnu",
];

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function writeJson(filePath, value) {
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

const packagePath = path.resolve(process.argv[2] || "package.json");
const pkg = readJson(packagePath);
const version = process.argv[3] || pkg.version;
if (!version) throw new Error("missing package version");

pkg.optionalDependencies = pkg.optionalDependencies || {};
for (const name of PLATFORM_DEPENDENCIES) {
  pkg.optionalDependencies[name] = version;
}

writeJson(packagePath, pkg);
process.stdout.write(`added platform optional dependencies to ${packagePath}\n`);
