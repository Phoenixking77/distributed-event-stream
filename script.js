#!/usr/bin/env node
import fs from 'fs';
import path from 'path';

function walk(dir) {
  const entries = fs.readdirSync(dir, { withFileTypes: true });

  for (const entry of entries) {
    const full = path.join(dir, entry.name);

    if (entry.isDirectory()) {
      walk(full);
    } else if (/\.(ts|js)$/.test(entry.name)) {
      stripFile(full);
    }
  }
}

function stripFile(filePath) {
  const src = fs.readFileSync(filePath, "utf8");
  const out = stripComments(src);

  if (out !== src) {
    fs.writeFileSync(filePath, out, "utf8");
    console.log("Stripped comments:", filePath);
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

// IMPORTANT FIX: use current working directory
const root = path.join(process.cwd(), "src");

if (!fs.existsSync(root)) {
  console.error("src directory not found at:", root);
  process.exit(1);
}

walk(root);
console.log("Done.");
