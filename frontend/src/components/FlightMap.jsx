import { useEffect, useMemo, useRef, useState } from "react";
import * as d3 from "d3";
import * as topojson from "topojson-client";
import { useFlightData, ConnectionStatus } from "../hooks/useFlightData";
import { getIATACode } from "../utils/airportCodes";
import { delaySeverity } from "../utils/delaySeverity";
import FlightDetail from "./FlightDetail";

const airports = {
  // ── US West Coast ──
  SFO:{lat:37.62,lon:-122.38},LAX:{lat:33.94,lon:-118.41},SEA:{lat:47.45,lon:-122.31},
  PDX:{lat:45.59,lon:-122.60},LAS:{lat:36.08,lon:-115.15},PHX:{lat:33.44,lon:-112.01},
  SAN:{lat:32.73,lon:-117.19},SJC:{lat:37.36,lon:-121.93},OAK:{lat:37.72,lon:-122.22},
  SMF:{lat:38.70,lon:-121.59},BUR:{lat:34.20,lon:-118.36},LGB:{lat:33.82,lon:-118.15},
  SNA:{lat:33.68,lon:-117.87},ONT:{lat:34.06,lon:-117.60},SBA:{lat:34.43,lon:-119.84},
  SBP:{lat:35.24,lon:-120.64},FAT:{lat:36.78,lon:-119.72},RNO:{lat:39.50,lon:-119.77},
  // ── US Mountain ──
  DEN:{lat:39.86,lon:-104.67},SLC:{lat:40.79,lon:-111.98},ABQ:{lat:35.04,lon:-106.61},
  ELP:{lat:31.81,lon:-106.38},BOI:{lat:43.56,lon:-116.22},TUS:{lat:32.12,lon:-110.94},
  // ── US Southwest / Texas ──
  DFW:{lat:32.90,lon:-97.04},IAH:{lat:29.98,lon:-95.34},HOU:{lat:29.65,lon:-95.28},
  SAT:{lat:29.53,lon:-98.47},AUS:{lat:30.20,lon:-97.67},DAL:{lat:32.85,lon:-96.85},
  // ── US Midwest ──
  ORD:{lat:41.97,lon:-87.91},MDW:{lat:41.79,lon:-87.74},DTW:{lat:42.21,lon:-83.35},
  MSP:{lat:44.88,lon:-93.22},STL:{lat:38.75,lon:-90.37},MKE:{lat:42.95,lon:-87.90},
  CLE:{lat:41.41,lon:-81.85},CMH:{lat:39.99,lon:-82.89},IND:{lat:39.72,lon:-86.29},
  CVG:{lat:39.05,lon:-84.67},MSN:{lat:43.14,lon:-89.34},OMA:{lat:41.30,lon:-95.90},
  DSM:{lat:41.53,lon:-93.66},MCI:{lat:39.30,lon:-94.71},
  // ── US Southeast ──
  ATL:{lat:33.64,lon:-84.43},MIA:{lat:25.79,lon:-80.29},FLL:{lat:26.07,lon:-80.15},
  MCO:{lat:28.43,lon:-81.31},TPA:{lat:27.98,lon:-82.53},JAX:{lat:30.49,lon:-81.69},
  PBI:{lat:26.68,lon:-80.10},RSW:{lat:26.54,lon:-81.76},SRQ:{lat:27.40,lon:-82.55},
  BNA:{lat:36.12,lon:-86.68},MEM:{lat:35.04,lon:-89.98},BHM:{lat:33.56,lon:-86.75},
  MSY:{lat:29.99,lon:-90.26},LIT:{lat:34.73,lon:-92.22},CHS:{lat:32.90,lon:-80.04},
  RDU:{lat:35.88,lon:-78.79},CLT:{lat:35.21,lon:-80.94},GSO:{lat:36.10,lon:-79.94},
  // ── US Northeast ──
  JFK:{lat:40.64,lon:-73.78},LGA:{lat:40.78,lon:-73.87},EWR:{lat:40.69,lon:-74.17},
  BOS:{lat:42.36,lon:-71.01},PHL:{lat:39.87,lon:-75.24},DCA:{lat:38.85,lon:-77.04},
  IAD:{lat:38.94,lon:-77.45},BWI:{lat:39.18,lon:-76.67},PIT:{lat:40.49,lon:-80.23},
  BDL:{lat:41.94,lon:-72.68},MHT:{lat:42.93,lon:-71.44},PVD:{lat:41.73,lon:-71.43},
  SYR:{lat:43.11,lon:-76.11},ALB:{lat:42.75,lon:-73.80},BUF:{lat:42.94,lon:-78.73},
  ROC:{lat:43.12,lon:-77.67},ORF:{lat:36.90,lon:-76.02},RIC:{lat:37.50,lon:-77.32},
  // ── US Alaska / Hawaii ──
  ANC:{lat:61.17,lon:-150.00},FAI:{lat:64.82,lon:-147.86},
  HNL:{lat:21.33,lon:-157.92},OGG:{lat:20.90,lon:-156.43},KOA:{lat:19.74,lon:-156.04},
  LIH:{lat:21.98,lon:-159.34},ITO:{lat:19.72,lon:-155.05},
  // ── Caribbean / Latin America ──
  SJU:{lat:18.44,lon:-66.00},CUN:{lat:21.04,lon:-86.87},MEX:{lat:19.44,lon:-99.07},
  GDL:{lat:20.52,lon:-103.31},MTY:{lat:25.78,lon:-100.11},BOG:{lat:4.70,lon:-74.15},
  GRU:{lat:-23.43,lon:-46.47},EZE:{lat:-34.82,lon:-58.54},LIM:{lat:-12.02,lon:-77.11},
  SCL:{lat:-33.39,lon:-70.79},PTY:{lat:9.07,lon:-79.38},
  // ── Europe ──
  LHR:{lat:51.47,lon:-0.46},CDG:{lat:49.01,lon:2.55},FRA:{lat:50.04,lon:8.56},
  AMS:{lat:52.31,lon:4.77},MAD:{lat:40.47,lon:-3.56},FCO:{lat:41.80,lon:12.24},
  ZRH:{lat:47.46,lon:8.55},MUC:{lat:48.35,lon:11.79},VIE:{lat:48.11,lon:16.57},
  BRU:{lat:50.90,lon:4.48},CPH:{lat:55.63,lon:12.66},HEL:{lat:60.32,lon:24.96},
  ARN:{lat:59.65,lon:17.92},OSL:{lat:60.20,lon:11.08},LIS:{lat:38.78,lon:-9.14},
  DUB:{lat:53.42,lon:-6.27},MAN:{lat:53.35,lon:-2.27},
  // ── Asia Pacific ──
  NRT:{lat:35.77,lon:140.39},HND:{lat:35.55,lon:139.78},ICN:{lat:37.46,lon:126.44},
  PEK:{lat:40.08,lon:116.58},PVG:{lat:31.14,lon:121.80},HKG:{lat:22.31,lon:113.91},
  SIN:{lat:1.36,lon:103.99},BKK:{lat:13.69,lon:100.75},KUL:{lat:2.75,lon:101.71},
  SYD:{lat:-33.95,lon:151.18},MEL:{lat:-37.67,lon:144.84},AKL:{lat:-37.01,lon:174.79},
  DEL:{lat:28.57,lon:77.09},BOM:{lat:19.09,lon:72.87},
  // ── Middle East / Africa ──
  DXB:{lat:25.25,lon:55.36},DOH:{lat:25.26,lon:51.57},AUH:{lat:24.44,lon:54.65},
  CAI:{lat:30.12,lon:31.41},NBO:{lat:-1.32,lon:36.93},JNB:{lat:-26.14,lon:28.25},
};

function planeColor(f, selected) {
  if (selected) return "#79c0ff";
  return delaySeverity(f.delay_minutes).color;
}

function bezierPoint(p1, ctrl, p2, t) {
  const mt = 1 - t;
  return [mt*mt*p1[0]+2*mt*t*ctrl[0]+t*t*p2[0], mt*mt*p1[1]+2*mt*t*ctrl[1]+t*t*p2[1]];
}

function bezierTangent(p1, ctrl, p2, t) {
  const mt = 1 - t;
  return [2*mt*(ctrl[0]-p1[0])+2*t*(p2[0]-ctrl[0]), 2*mt*(ctrl[1]-p1[1])+2*t*(p2[1]-ctrl[1])];
}

function ctrlPoint(p1, p2) {
  return [(p1[0]+p2[0])/2, (p1[1]+p2[1])/2 - 35];
}

function parseDate(dateStr) {
  if (!dateStr) return null;
  if (dateStr instanceof Date) return dateStr;
  let clean = dateStr.trim();
  if (clean.includes(" ") && !clean.includes("T")) {
    clean = clean.replace(" ", "T");
  }
  const tzRegex = /([+-]\d{2})$/;
  if (tzRegex.test(clean)) {
    clean = clean + ":00";
  }
  const d = new Date(clean);
  return isNaN(d.getTime()) ? null : d;
}

function flightPosition(f) {
  const dep = parseDate(f.scheduled_departure);
  const arr = parseDate(f.scheduled_arrival);
  if (!dep || !arr) return 0.45;
  const t = (Date.now() - dep.getTime()) / (arr.getTime() - dep.getTime());
  return Math.max(0.05, Math.min(0.95, t));
}

const STATUS_BADGE = {
  [ConnectionStatus.CONNECTING]: { label: "● Connecting…", bg: "#2d2410", fg: "#e3b341" },
  [ConnectionStatus.CONNECTED]: { label: "● Live", bg: "#1a2a1a", fg: "#56d364" },
  [ConnectionStatus.RECONNECTING]: { label: "● Reconnecting…", bg: "#2d2410", fg: "#e3b341" },
  [ConnectionStatus.DISCONNECTED]: { label: "● Disconnected", bg: "#3d1515", fg: "#ff7b72" },
};

// Cascade overlay coloring: hop 1 skews orange, later hops skew yellow —
// same idea as the mockup in the Phase G brief ("source red, affected
// orange -> yellow, fading"), computed rather than hardcoded per hop count
// so it degrades gracefully past a handful of hops instead of running out
// of colors.
function cascadeHopColor(hops) {
  const t = Math.min(1, (hops - 1) / 3);
  const from = [255, 140, 66]; // #ff8c42
  const to = [255, 210, 63]; // #ffd23f
  const rgb = from.map((c, i) => Math.round(c + (to[i] - c) * t));
  return `rgb(${rgb.join(",")})`;
}

export default function FlightMap() {
  const svgRef = useRef(null);
  const containerRef = useRef(null);
  const [worldData, setWorldData] = useState(null);
  const [width, setWidth] = useState(900);
  const [hoveredCascadeId, setHoveredCascadeId] = useState(null);

  const {
    targetAirport,
    connectionStatus,
    hardFailure,
    snapshotLoaded,
    flights: flightsById,
    predictions,
    gateReassignments,
    selectedFlightId,
    setSelectedFlightId,
    getPropagationChain,
  } = useFlightData();

  const flights = useMemo(() => Array.from(flightsById.values()), [flightsById]);

  // load world map once
  useEffect(() => {
    fetch("/countries-110m.json")
      .then(r => r.json())
      .then(setWorldData)
      .catch(err => {
        console.error("Failed to load local map, falling back to CDN:", err);
        fetch("https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json")
          .then(r => r.json())
          .then(setWorldData);
      });
  }, []);

  // Responsive width (task 12): the SVG's own viewBox already scales its
  // *contents*, but the side panel's layout (row vs. stacked) needs a real
  // breakpoint, so track container width via ResizeObserver.
  useEffect(() => {
    if (!containerRef.current) return undefined;
    const el = containerRef.current;
    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) setWidth(entry.contentRect.width);
    });
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  const isNarrow = width < 720;

  const selectedFlight = flightsById.get(selectedFlightId) || null;
  const { upstream, downstream } = useMemo(
    () => (selectedFlightId ? getPropagationChain(selectedFlightId) : { upstream: null, downstream: [] }),
    [selectedFlightId, getPropagationChain]
  );

  // Keyboard navigation (task 13): arrow keys step through the currently
  // rendered flight list and select one, without requiring a mouse.
  function handleMapKeyDown(e) {
    if (!["ArrowRight", "ArrowLeft", "ArrowUp", "ArrowDown"].includes(e.key)) return;
    e.preventDefault();
    if (flights.length === 0) return;
    const ids = flights.map((f) => f.flight_id);
    const currentIdx = ids.indexOf(selectedFlightId);
    const delta = e.key === "ArrowRight" || e.key === "ArrowDown" ? 1 : -1;
    const nextIdx = currentIdx === -1 ? 0 : (currentIdx + delta + ids.length) % ids.length;
    setSelectedFlightId(ids[nextIdx]);
  }

  // Everything the render loop below needs, mirrored into refs. This is
  // the fix for a real performance bug found via live testing against
  // ~1700 real flights: WS messages arrive batched every 100ms
  // (useFlightData's own batching), and `flightsById` gets a new Map
  // reference on every batch — with the D3 effect depending on it
  // directly, the *entire* redraw (position/rotation trig + attribute
  // writes for every plane, plus a full cascade-overlay rebuild
  // restarting all its transitions) was re-running up to 10x/second. At
  // ~1700 flights that was enough sustained main-thread work to starve
  // requestAnimationFrame entirely (confirmed: a trivial rAF-loop
  // measurement timed out against the live page). Refs let the render
  // loop read the latest data without the *effect itself* re-running on
  // every data tick — it now runs on its own fixed, bounded cadence
  // instead (see the interval below), which is what actually decouples
  // "how often data arrives" from "how often we redraw."
  const flightsByIdRef = useRef(flightsById);
  const selectedFlightIdRef = useRef(selectedFlightId);
  const gateReassignmentsRef = useRef(gateReassignments);
  const hoveredCascadeIdRef = useRef(hoveredCascadeId);
  const cascadeRef = useRef({ selectedFlight: null, upstream: null, downstream: [] });
  useEffect(() => { flightsByIdRef.current = flightsById; }, [flightsById]);
  useEffect(() => { selectedFlightIdRef.current = selectedFlightId; }, [selectedFlightId]);
  useEffect(() => { gateReassignmentsRef.current = gateReassignments; }, [gateReassignments]);
  useEffect(() => { hoveredCascadeIdRef.current = hoveredCascadeId; }, [hoveredCascadeId]);
  useEffect(() => {
    cascadeRef.current = { selectedFlight, upstream, downstream };
  }, [selectedFlight, upstream, downstream]);

  // d3 render — set up once per worldData load, then redraws on a fixed
  // 500ms interval (comfortably smooth for markers that move across the
  // whole map over tens of minutes, and bounds worst-case work to 2 full
  // passes/sec instead of one per WS batch) rather than on every React
  // re-render.
  useEffect(() => {
    if (!worldData) return undefined;
    const W = 900, H = 500;
    const svg = d3.select(svgRef.current);
    svg.attr("viewBox", `0 0 ${W} ${H}`);

    const proj = d3.geoNaturalEarth1().scale(140).translate([W/2, H/2]);
    const path = d3.geoPath(proj);

    function project(code) {
      const a = airports[getIATACode(code)];
      return a ? proj([a.lon, a.lat]) : null;
    }

    svg.selectAll("*").remove();
    svg.append("rect").attr("width", W).attr("height", H).attr("fill", "#0a0f1a");
    svg.append("g").attr("class", "countries").selectAll("path")
      .data(topojson.feature(worldData, worldData.objects.countries).features)
      .join("path").attr("d", path)
      .attr("fill", "#161d2e").attr("stroke", "#1e2940").attr("stroke-width", 0.4);
    const routeG = svg.append("g").attr("class", "routes");
    const apG = svg.append("g").attr("class", "airports-layer");
    const cascadeG = svg.append("g").attr("class", "cascade");
    const planeG = svg.append("g").attr("class", "planes");

    let lastCascadeKey = null;

    function renderFrame() {
      const flightsByIdNow = flightsByIdRef.current;
      const displayFlights = Array.from(flightsByIdNow.values());
      const selectedFlightIdNow = selectedFlightIdRef.current;
      const gateReassignmentsNow = gateReassignmentsRef.current;
      const hoveredCascadeIdNow = hoveredCascadeIdRef.current;

      // Routes: keyed join by flight_id so unrelated flights don't get
      // torn down and rebuilt just because some other flight changed.
      routeG.selectAll("path")
        .data(
          displayFlights.filter((f) => project(f.origin) && project(f.destination)),
          (f) => f.flight_id
        )
        .join("path")
        .attr("d", (f) => {
          const p1 = project(f.origin), p2 = project(f.destination);
          const ctrl = ctrlPoint(p1, p2);
          return `M${p1[0]},${p1[1]} Q${ctrl[0]},${ctrl[1]} ${p2[0]},${p2[1]}`;
        })
        .attr("fill", "none")
        .attr("stroke", (f) => (f.delay_minutes > 0 ? "#ff7b72" : "#388bfd"))
        .attr("stroke-width", (f) => (f.delay_minutes > 0 ? 1.8 : 1.0))
        .attr("stroke-opacity", (f) => (f.delay_minutes > 0 ? 0.70 : 0.45))
        .attr("stroke-dasharray", (f) => (f.delay_minutes > 0 ? "none" : "4,3"));

      const apSet = new Set(displayFlights.flatMap((f) => [f.origin, f.destination]).filter(Boolean));
      apG.selectAll("g").data(Array.from(apSet), (d) => d).join((enter) => {
        const g = enter.append("g");
        g.each(function (code) {
          const pt = project(code);
          if (!pt) return;
          const [x, y] = pt;
          const s = 4;
          const sel = d3.select(this);
          sel.append("line").attr("x1",x).attr("y1",y-s).attr("x2",x).attr("y2",y+s)
            .attr("stroke","#388bfd").attr("stroke-width",1).attr("stroke-opacity",0.7);
          sel.append("line").attr("x1",x-s).attr("y1",y).attr("x2",x+s).attr("y2",y)
            .attr("stroke","#388bfd").attr("stroke-width",1).attr("stroke-opacity",0.7);
          sel.append("text").attr("x",x+7).attr("y",y+3.5)
            .attr("font-size",8).attr("fill","#8b949e").text(getIATACode(code));
        });
        return g;
      });

      // Planes: keyed join by flight_id.
      const planes = planeG.selectAll("g.plane")
        .data(
          displayFlights.filter((f) => project(f.origin) && project(f.destination)),
          (f) => f.flight_id
        )
        .join((enter) => {
          const g = enter.append("g").attr("class", "plane");
          g.append("circle").attr("class", "select-ring").attr("r", 13).attr("fill", "none")
            .attr("stroke", "#79c0ff").attr("stroke-width", 1).attr("stroke-opacity", 0);
          const rotated = g.append("g").attr("class", "rotated");
          rotated.append("path").attr("class", "body").attr("d", "M0,-9 L5,5 L0,2.5 L-5,5 Z")
            .attr("stroke", "#0d1117").attr("stroke-width", 0.8);
          g.append("circle").attr("class", "delay-badge").attr("cx",8).attr("cy",-8).attr("r",5)
            .attr("fill","#ff7b72").attr("stroke","#0d1117").attr("stroke-width",0.5).attr("opacity", 0);
          g.append("text").attr("class", "delay-badge-text").attr("x",8).attr("y",-5)
            .attr("text-anchor","middle").attr("font-size",6).attr("font-weight",700)
            .attr("fill","#0d1117").text("!").attr("opacity", 0);
          g.append("rect").attr("class", "gate-badge").attr("x", -13).attr("y", -13)
            .attr("width", 6).attr("height", 6).attr("fill", "#e3b341")
            .attr("stroke", "#0d1117").attr("stroke-width", 0.4).attr("opacity", 0);
          return g;
        })
        .attr("cursor", "pointer")
        .attr("tabindex", 0)
        .attr("role", "button")
        .attr("aria-label", (f) => `Flight ${f.airline_code}${f.flight_number}, ${getIATACode(f.origin)} to ${getIATACode(f.destination)}, ${f.delay_minutes > 0 ? `delayed ${f.delay_minutes} minutes` : "on time"}`)
        .on("click", (event, f) => setSelectedFlightId((prev) => (prev === f.flight_id ? null : f.flight_id)))
        .on("keydown", (event, f) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            setSelectedFlightId((prev) => (prev === f.flight_id ? null : f.flight_id));
          }
        });

      planes.each(function (f) {
        const g = d3.select(this);
        const p1 = project(f.origin), p2 = project(f.destination);
        const ctrl = ctrlPoint(p1, p2);
        const t = flightPosition(f);
        const pos = bezierPoint(p1, ctrl, p2, t);
        const tan = bezierTangent(p1, ctrl, p2, t);
        const angle = Math.atan2(tan[1], tan[0]) * 180 / Math.PI + 90;
        const isSel = selectedFlightIdNow === f.flight_id;
        const col = planeColor(f, isSel);

        g.attr("transform", `translate(${pos[0]},${pos[1]})`);
        g.select(".select-ring").attr("stroke-opacity", isSel ? 0.5 : 0);
        g.select(".rotated").attr("transform", `rotate(${angle})`);
        g.select(".body").attr("fill", col);
        const delayed = f.delay_minutes > 0;
        g.select(".delay-badge").attr("opacity", delayed ? 1 : 0);
        g.select(".delay-badge-text").attr("opacity", delayed ? 1 : 0);
        g.select(".gate-badge").attr("opacity", gateReassignmentsNow.get(f.flight_id) ? 1 : 0);
      });

      // Propagation cascade overlay (task 5): small dataset, but only
      // rebuilt (and its entrance transitions restarted) when the actual
      // selection/chain identity changes — not every render tick — so
      // the source pulse's own indefinite <animate> loop isn't chopped
      // off mid-cycle every 500ms.
      const { selectedFlight: selFlight, upstream: up, downstream: down } = cascadeRef.current;
      const cascadeKey = selFlight
        ? `${selFlight.flight_id}|${down.map((d) => `${d.flight_id}:${d.hops}`).join(",")}|${up?.flight_id || ""}|${hoveredCascadeIdNow || ""}`
        : null;

      if (cascadeKey !== lastCascadeKey) {
        lastCascadeKey = cascadeKey;
        cascadeG.selectAll("*").remove();

        if (selFlight) {
          const sourcePos = project(selFlight.origin) && project(selFlight.destination)
            ? bezierPoint(
                project(selFlight.origin),
                ctrlPoint(project(selFlight.origin), project(selFlight.destination)),
                project(selFlight.destination),
                flightPosition(selFlight)
              )
            : null;

          if (sourcePos && down.length > 0) {
            const pulse = cascadeG.append("circle")
              .attr("cx", sourcePos[0]).attr("cy", sourcePos[1]).attr("r", 9)
              .attr("fill", "none").attr("stroke", "#f85149").attr("stroke-width", 1.5);
            pulse.append("animate").attr("attributeName", "r").attr("values", "9;20;9").attr("dur", "1.6s").attr("repeatCount", "indefinite");
            pulse.append("animate").attr("attributeName", "stroke-opacity").attr("values", "0.9;0;0.9").attr("dur", "1.6s").attr("repeatCount", "indefinite");

            down.forEach((d, i) => {
              const target = flightsByIdNow.get(d.flight_id);
              if (!target) return;
              const p1 = project(target.origin), p2 = project(target.destination);
              if (!p1 || !p2) return;
              const targetPos = bezierPoint(p1, ctrlPoint(p1, p2), p2, flightPosition(target));
              const color = cascadeHopColor(d.hops || 1);
              const isHovered = hoveredCascadeIdNow === d.flight_id;

              const link = cascadeG.append("line")
                .attr("x1", sourcePos[0]).attr("y1", sourcePos[1])
                .attr("x2", targetPos[0]).attr("y2", targetPos[1])
                .attr("stroke", color)
                .attr("stroke-width", isHovered ? 2.2 : 1.2)
                .attr("stroke-opacity", 0)
                .attr("stroke-dasharray", "3,2");
              link.transition().delay(i * 150).duration(400).attr("stroke-opacity", isHovered ? 0.95 : 0.6);

              const decay = Math.round(0.75 ** (d.hops || 1) * 100);
              const mid = [(sourcePos[0] + targetPos[0]) / 2, (sourcePos[1] + targetPos[1]) / 2];
              cascadeG.append("text")
                .attr("x", mid[0]).attr("y", mid[1] - 3)
                .attr("font-size", 7).attr("fill", color).attr("text-anchor", "middle")
                .attr("opacity", 0)
                .text(`+${d.delay_minutes}m · ${decay}%`)
                .transition().delay(i * 150).duration(400).attr("opacity", 0.9);

              const ring = cascadeG.append("circle")
                .attr("cx", targetPos[0]).attr("cy", targetPos[1])
                .attr("r", isHovered ? 12 : 9)
                .attr("fill", "none").attr("stroke", color).attr("stroke-width", isHovered ? 2 : 1.3)
                .attr("opacity", 0)
                .style("cursor", "pointer")
                .on("mouseenter", () => setHoveredCascadeId(d.flight_id))
                .on("mouseleave", () => setHoveredCascadeId((prev) => (prev === d.flight_id ? null : prev)))
                .on("click", () => setSelectedFlightId(d.flight_id));
              ring.transition().delay(i * 150).duration(400).attr("opacity", 1);

              if (gateReassignmentsNow.get(d.flight_id)) {
                cascadeG.append("rect")
                  .attr("x", targetPos[0] + 6).attr("y", targetPos[1] - 6)
                  .attr("width", 7).attr("height", 7)
                  .attr("fill", "#e3b341").attr("stroke", "#0d1117").attr("stroke-width", 0.5)
                  .attr("opacity", 0)
                  .transition().delay(i * 150).duration(400).attr("opacity", 1);
              }
            });
          }

          if (sourcePos && up) {
            const src = flightsByIdNow.get(up.flight_id);
            if (src) {
              const p1 = project(src.origin), p2 = project(src.destination);
              if (p1 && p2) {
                const upstreamPos = bezierPoint(p1, ctrlPoint(p1, p2), p2, flightPosition(src));
                cascadeG.append("line")
                  .attr("x1", upstreamPos[0]).attr("y1", upstreamPos[1])
                  .attr("x2", sourcePos[0]).attr("y2", sourcePos[1])
                  .attr("stroke", "#e3b341").attr("stroke-width", 1).attr("stroke-opacity", 0.5)
                  .attr("stroke-dasharray", "2,3");
              }
            }
          }
        }
      }
    }

    renderFrame();
    const interval = setInterval(renderFrame, 500);
    return () => clearInterval(interval);
  }, [worldData, setSelectedFlightId]);

  const delayed = flights.filter(f => f.delay_minutes > 0).length;
  const total = flights.length;
  const airportLabel = targetAirport ? getIATACode(targetAirport) : "…";
  const badge = STATUS_BADGE[connectionStatus] || STATUS_BADGE[ConnectionStatus.CONNECTING];

  const selectedPrediction = selectedFlightId ? predictions.get(selectedFlightId) : null;
  const selectedGateReassignment = selectedFlightId ? gateReassignments.get(selectedFlightId) : null;

  return (
    <div
      ref={containerRef}
      style={{background:"#0d1117",borderRadius:12,overflow:"hidden",border:"0.5px solid #30363d",fontFamily:"sans-serif",color:"#e6edf3"}}
    >
      <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",padding:"10px 16px",background:"#161b22",borderBottom:"0.5px solid #30363d",flexWrap:"wrap",gap:6}}>
        <div style={{display:"flex",alignItems:"center",gap:10,flexWrap:"wrap"}}>
          <span style={{fontSize:13,fontWeight:500}}>FlightTracker — {airportLabel}</span>
          <span style={{fontSize:11,padding:"2px 8px",borderRadius:20,background:"#0d1f3c",color:"#79c0ff"}}>{total} flights</span>
          <span style={{fontSize:11,padding:"2px 8px",borderRadius:20,background:"#3d1515",color:"#ff7b72"}}>{delayed} delayed</span>
          <span style={{fontSize:11,padding:"2px 8px",borderRadius:20,background:badge.bg,color:badge.fg}} role="status" aria-live="polite">{badge.label}</span>
        </div>
        <span style={{fontSize:11,color:"#8b949e"}}>Click a plane, or use arrow keys, to inspect</span>
      </div>

      <div style={{position:"relative", display: isNarrow ? "block" : "flex"}}>
        <div style={{position:"relative", flex: "1 1 auto", minWidth: 0}}>
          <svg
            ref={svgRef}
            role="img"
            aria-label={`Map of ${total} tracked flights around ${airportLabel}, ${delayed} delayed. Use arrow keys to cycle through flights.`}
            tabIndex={0}
            onKeyDown={handleMapKeyDown}
            style={{width:"100%",height:480,display:"block",outline:"none"}}
          />
          {!snapshotLoaded && (
            <div style={{position:"absolute",inset:0,display:"flex",alignItems:"center",justifyContent:"center",background:"#0a0f1acc"}}>
              <div style={{display:"flex",flexDirection:"column",alignItems:"center",gap:8}}>
                <div style={{width:24,height:24,border:"2px solid #30363d",borderTopColor:"#58a6ff",borderRadius:"50%",animation:"ft-spin 0.8s linear infinite"}} />
                <span style={{fontSize:12,color:"#8b949e"}}>Loading flight snapshot…</span>
              </div>
            </div>
          )}
          {snapshotLoaded && total === 0 && (
            <div style={{position:"absolute",top:12,left:12,fontSize:12,color:"#8b949e",background:"#161b22cc",border:"0.5px solid #30363d",borderRadius:8,padding:"6px 10px"}}>
              Waiting for flights from the backend…
            </div>
          )}
          {hardFailure && (
            <div style={{position:"absolute",inset:0,display:"flex",alignItems:"center",justifyContent:"center",background:"#0a0f1aee"}}>
              <div style={{textAlign:"center",fontSize:12,color:"#ff7b72",background:"#161b22",border:"0.5px solid #3d1515",borderRadius:8,padding:"14px 18px"}}>
                Connection failed.<br/>
                <span style={{color:"#8b949e",fontSize:11}}>Refresh to retry.</span>
              </div>
            </div>
          )}
          {!hardFailure && connectionStatus === ConnectionStatus.RECONNECTING && (
            <div style={{position:"absolute",bottom:12,left:12,fontSize:11,color:"#e3b341",background:"#161b22cc",border:"0.5px solid #30363d",borderRadius:8,padding:"5px 10px"}}>
              Reconnecting…
            </div>
          )}
        </div>

        <div style={{
          width: isNarrow ? "auto" : 260,
          maxHeight: isNarrow ? 320 : 480,
          overflowY: "auto",
          background:"#161b22",
          borderLeft: isNarrow ? "none" : "0.5px solid #30363d",
          borderTop: isNarrow ? "0.5px solid #30363d" : "none",
        }}>
          <div style={{padding:"8px 12px",borderBottom:"0.5px solid #30363d",fontSize:11,color:"#8b949e",textTransform:"uppercase",letterSpacing:".07em"}}>Flight detail</div>
          <div style={{padding:"10px 12px"}}>
            <FlightDetail
              flight={selectedFlight}
              prediction={selectedPrediction}
              upstream={upstream}
              downstream={downstream}
              gateReassignment={selectedGateReassignment}
              flights={flightsById}
              gateReassignments={gateReassignments}
              onSelectFlight={setSelectedFlightId}
            />
          </div>
        </div>
      </div>

      <div style={{display:"flex",flexWrap:"wrap",gap:10,padding:"8px 16px",background:"#161b22",borderTop:"0.5px solid #30363d"}}>
        {[["#58a6ff","On time"],["#d29922","Minor delay"],["#e8871e","Moderate delay"],["#f85149","Severe delay"],["#79c0ff","Selected"]].map(([col,label]) => (
          <div key={label} style={{display:"flex",alignItems:"center",gap:5,fontSize:10,color:"#8b949e"}}>
            <svg width="10" height="10" viewBox="0 0 10 10"><circle cx="5" cy="5" r="4" fill={col}/></svg>
            {label}
          </div>
        ))}
      </div>
      <style>{"@keyframes ft-spin { to { transform: rotate(360deg); } }"}</style>
    </div>
  );
}
