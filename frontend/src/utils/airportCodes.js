// Shared with FlightMap.jsx, FlightDetail.jsx, GateMap.jsx — moved here
// (Phase G) so the ICAO->IATA display logic isn't duplicated across
// components that all need to render the same "KJFK" -> "JFK" label.
export function getIATACode(icaoOrIata) {
  if (!icaoOrIata) return "";
  const code = icaoOrIata.toUpperCase();
  if (code.length === 3) return code;
  if (code.length === 4) {
    if (code === "EGLL") return "LHR";
    if (code === "RJAA") return "NRT";
    if (code === "TJSJ") return "SJU";
    if (code.startsWith("K")) return code.slice(1);
  }
  return code;
}
