#!/usr/bin/env node
const fs = require("fs");
const path = require("path");

function walk(dir) {
  const entries = fs.readdirSync(dir, { withFileTypes: true });

  for (const entry of entries) {
    const full = path.join(dir, entry.name);

    if (entry.isDirectory()) {
      // skip common large/irrelevant folders
      if (["node_modules", ".git", "dist", "build", "test-data", "types"].includes(entry.name)) continue;
      walk(full);
    } else if (/\.(ts|js)$/.test(entry.name) && !entry.name.endsWith('.d.ts')) {
      stripFile(full);
    }
  }
}

function stripFile(filePath) {
  let src;
  try {
    src = fs.readFileSync(filePath, "utf8");
  } catch (err) {
    console.error("Could not read", filePath, err.message);
    return;
  }

  const out = stripComments(src);

  if (out !== src) {
    try {
      fs.writeFileSync(filePath, out, "utf8");
      console.log("Stripped comments:", filePath);
    } catch (err) {
      console.error("Could not write", filePath, err.message);
    }
  }
}

// Robust comment stripper that preserves strings and regex literals
function stripComments(input) {
  let i = 0;
  const len = input.length;
  let out = "";

  while (i < len) {
    const ch = input[i];
    const ch2 = input[i + 1];

    // single-line comment
    if (ch === "/" && ch2 === "/") {
      i += 2;
      while (i < len && input[i] !== "\n") i++;
      continue;
    }

    // block comment
    if (ch === "/" && ch2 === "*") {
      i += 2;
      while (i < len && !(input[i] === "*" && input[i + 1] === "/")) i++;
      i += 2;
      continue;
    }

    // normal string literals
    if (ch === '"' || ch === "'") {
      const quote = ch;
      out += ch;
      i++;

      while (i < len) {
        const c = input[i];
        out += c;

        if (c === "\\") {
          i++;
          if (i < len) out += input[i];
          i++;
          continue;
        }

        if (c === quote) {
          i++;
          break;
        }

        i++;
      }
      continue;
    }

    // template literal
    if (ch === "`") {
      out += ch;
      i++;

      while (i < len) {
        const c = input[i];
        out += c;

        if (c === "\\") {
          i++;
          if (i < len) out += input[i];
          i++;
          continue;
        }

        if (c === "`") {
          i++;
          break;
        }

        // handle ${ ... } inside template literal without confusing the parser
        if (c === "$" && input[i + 1] === "{") {
          i += 2; // consume ${
          out += '{';
          // copy until matching }
          let depth = 1;
          while (i < len && depth > 0) {
            const d = input[i];
            out += d;
            if (d === "{") depth++;
            else if (d === "}") depth--;
            i++;
          }
          continue;
        }

        i++;
      }
      continue;
    }

    // regex literal detection
    if (ch === "/") {
      let j = out.length - 1;
      while (j >= 0 && /\s/.test(out[j])) j--;
      const prevChar = j >= 0 ? out[j] : "";

      if (prevChar === "" || "([=:{,;!&|?+-~*%^<>".includes(prevChar)) {
        out += ch;
        i++;

        while (i < len) {
          const c = input[i];
          out += c;

          if (c === "\\") {
            i++;
            if (i < len) out += input[i];
            i++;
            continue;
          }

          if (c === "/") {
            i++;
            break;
          }

          i++;
        }
        continue;
      }
    }

    out += ch;
    i++;
  }

  return out;
}

function findRoots(args) {
  if (args.length) return args.map(a => path.resolve(a));

  const cwd = process.cwd();
  const roots = [];

  if (fs.existsSync(path.join(cwd, 'src'))) roots.push(path.join(cwd, 'src'));

  const top = fs.readdirSync(cwd, { withFileTypes: true });
  for (const e of top) {
    if (e.isDirectory()) {
      const p = path.join(cwd, e.name, 'src');
      if (fs.existsSync(p)) roots.push(p);
    }
  }

  return roots.length ? roots : [cwd];
}

// main
const args = process.argv.slice(2);
const roots = findRoots(args);

for (const r of roots) {
  if (!fs.existsSync(r)) {
    console.error('root not found:', r);
    continue;
  }

  console.log('Processing:', r);
  walk(r);
}

console.log('Done.');
