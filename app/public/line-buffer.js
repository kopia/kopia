/**
 * Returns a function that joins chunks of process output into complete
 * lines. A chunk can end mid-line, for example a long notification JSON
 * split across pipe reads, so the partial line is held until the next chunk.
 */
export function lineBuffer() {
  let pending = "";

  return (chunk) => {
    const lines = (pending + chunk).split("\n");
    pending = lines.pop();
    return lines;
  };
}
