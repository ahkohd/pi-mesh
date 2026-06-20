import { spawnSync } from "node:child_process";
import { delimiter, dirname } from "node:path";
import { resolveBinary } from "./resolve-binary.js";

export function runBinary(name) {
  let binary;
  try {
    binary = resolveBinary(name);
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  }

  const env = {
    ...process.env,
    PATH: [dirname(binary), process.env.PATH].filter(Boolean).join(delimiter),
  };
  const result = spawnSync(binary, process.argv.slice(2), { stdio: "inherit", env });

  if (result.error) {
    console.error(result.error.message);
    process.exit(1);
  }

  process.exit(result.status ?? 1);
}
