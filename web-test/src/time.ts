/** Store times in MySQL `INT` as Unix seconds; milliseconds overflow signed INT. */
export function unixSecondsNow(): number {
  return Math.floor(Date.now() / 1000);
}
