import { useEffect, useRef, useState, useCallback } from "react";
import * as d3 from "d3";
import * as topojson from "topojson-client";

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

function getIATACode(icaoOrIata) {
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

function planeColor(f, selected) {
  if (selected) return "#79c0ff";
  if (f.propagated) return "#e3b341";
  if (f.delay_minutes > 0) return "#ff7b72";
  return "#56d364";
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

const AIRLINES = [
  {code:"UA",name:"United"},   {code:"AA",name:"American"}, {code:"DL",name:"Delta"},
  {code:"WN",name:"Southwest"},{code:"AS",name:"Alaska"},   {code:"B6",name:"JetBlue"},
  {code:"F9",name:"Frontier"}, {code:"NK",name:"Spirit"},   {code:"HA",name:"Hawaiian"},
  {code:"OO",name:"SkyWest"},
];
const IATA_LIST = Object.keys(airports).filter(a => a !== "SFO");

function generateMockFlights() {
  const now = Date.now();
  const flights = [];

  for (let i = 0; i < 100; i++) {
    const airline = AIRLINES[i % AIRLINES.length];
    const flightNum = String(100 + Math.floor(Math.random() * 9800));
    const isDeparture = i < 50;
    const other = IATA_LIST[Math.floor(Math.random() * IATA_LIST.length)];

    // origin / destination — SFO is always one end
    const origin = isDeparture ? "SFO" : other;
    const destination = isDeparture ? other : "SFO";

    // Departure time: -4h to +6h from now so planes are at various arc positions
    const depOffsetMs = (Math.random() * 10 - 4) * 3_600_000;
    const depTime = now + depOffsetMs;

    // Flight duration: 1 – 12 hours depending on rough distance
    const durationHrs = 1 + Math.random() * 11;
    const arrTime = depTime + durationHrs * 3_600_000;

    const delay = Math.random() < 0.3 ? Math.floor(10 + Math.random() * 110) : 0;

    const progress = (now - depTime) / (arrTime - depTime);
    const status = progress > 0.98 ? "landed" : progress > 0 ? "active" : "scheduled";

    const terminals = ["A","B","C","D","E","G","I"];
    const gate = `${terminals[Math.floor(Math.random()*terminals.length)]}${Math.floor(1+Math.random()*30)}`;

    flights.push({
      flight_key:          `${airline.code}${flightNum}-mock-${i}`,
      flight_id:           `${airline.code}${flightNum}-mock-${i}`,
      airline_code:        airline.code,
      flight_number:       flightNum,
      origin,
      destination,
      aircraft_id:         `N${Math.floor(10000 + Math.random() * 89999)}`,
      gate_id:             gate,
      scheduled_departure: new Date(depTime).toISOString(),
      scheduled_arrival:   new Date(arrTime).toISOString(),
      delay_minutes:       delay,
      status,
      propagated:          false,
    });
  }
  return flights;
}

export default function FlightMap() {
  const svgRef = useRef(null);
  const [selected, setSelected] = useState(null);
  const [worldData, setWorldData] = useState(null);
  const [flights] = useState(() => generateMockFlights());

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

  // d3 render
  useEffect(() => {
    if (!worldData) return;
    const W = 900, H = 500;
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();
    svg.attr("viewBox", `0 0 ${W} ${H}`);

    const proj = d3.geoNaturalEarth1().scale(140).translate([W/2, H/2]);
    const path = d3.geoPath(proj);

    function project(code) {
      const a = airports[getIATACode(code)];
      return a ? proj([a.lon, a.lat]) : null;
    }

    svg.append("rect").attr("width", W).attr("height", H).attr("fill", "#0a0f1a");

    svg.append("g").selectAll("path")
      .data(topojson.feature(worldData, worldData.objects.countries).features)
      .join("path").attr("d", path)
      .attr("fill", "#161d2e").attr("stroke", "#1e2940").attr("stroke-width", 0.4);

    const routeG = svg.append("g");
    const apG = svg.append("g");
    const planeG = svg.append("g");

    const displayFlights = flights.length > 0 ? flights : [];

    displayFlights.forEach(f => {
      const p1 = project(f.origin), p2 = project(f.destination);
      if (!p1 || !p2) return;
      const ctrl = ctrlPoint(p1, p2);
      const isDelayed = f.delay_minutes > 0;
      routeG.append("path")
        .attr("d", `M${p1[0]},${p1[1]} Q${ctrl[0]},${ctrl[1]} ${p2[0]},${p2[1]}`)
        .attr("fill", "none")
        .attr("stroke", isDelayed ? "#ff7b72" : "#388bfd")
        .attr("stroke-width", isDelayed ? 1.8 : 1.0)
        .attr("stroke-opacity", isDelayed ? 0.70 : 0.45)
        .attr("stroke-dasharray", isDelayed ? "none" : "4,3");
    });

    const apSet = new Set(displayFlights.flatMap(f => [f.origin, f.destination]).filter(Boolean));
    apSet.forEach(code => {
      const pt = project(code);
      if (!pt) return;
      const [x, y] = pt;
      const s = 4;
      apG.append("line").attr("x1",x).attr("y1",y-s).attr("x2",x).attr("y2",y+s)
        .attr("stroke","#388bfd").attr("stroke-width",1).attr("stroke-opacity",0.7);
      apG.append("line").attr("x1",x-s).attr("y1",y).attr("x2",x+s).attr("y2",y)
        .attr("stroke","#388bfd").attr("stroke-width",1).attr("stroke-opacity",0.7);
      apG.append("text").attr("x",x+7).attr("y",y+3.5)
        .attr("font-size",8).attr("fill","#8b949e").text(getIATACode(code));
    });

    displayFlights.forEach(f => {
      const p1 = project(f.origin), p2 = project(f.destination);
      if (!p1 || !p2) return;
      const ctrl = ctrlPoint(p1, p2);
      const t = flightPosition(f);
      const pos = bezierPoint(p1, ctrl, p2, t);
      const tan = bezierTangent(p1, ctrl, p2, t);
      const angle = Math.atan2(tan[1], tan[0]) * 180 / Math.PI + 90;
      const isSel = selected === f.flight_key;
      const col = planeColor(f, isSel);

      const g = planeG.append("g")
        .attr("transform", `translate(${pos[0]},${pos[1]})`)
        .attr("cursor", "pointer")
        .on("click", () => setSelected(prev => prev === f.flight_key ? null : f.flight_key));

      if (isSel) {
        g.append("circle").attr("r", 13).attr("fill", "none")
          .attr("stroke", "#79c0ff").attr("stroke-width", 1).attr("stroke-opacity", 0.5);
      }

      g.append("g").attr("transform", `rotate(${angle})`)
        .append("path").attr("d", "M0,-9 L5,5 L0,2.5 L-5,5 Z")
        .attr("fill", col).attr("stroke", "#0d1117").attr("stroke-width", 0.8);

      if (f.delay_minutes > 0) {
        g.append("circle").attr("cx",8).attr("cy",-8).attr("r",5)
          .attr("fill","#ff7b72").attr("stroke","#0d1117").attr("stroke-width",0.5);
        g.append("text").attr("x",8).attr("y",-5)
          .attr("text-anchor","middle").attr("font-size",6).attr("font-weight",700)
          .attr("fill","#0d1117").text("!");
      }
    });

  }, [worldData, flights, selected]);

  const selectedFlight = flights.find(f => f.flight_key === selected);
  const delayed = flights.filter(f => f.delay_minutes > 0).length;
  const total = flights.length;

  return (
    <div style={{background:"#0d1117",borderRadius:12,overflow:"hidden",border:"0.5px solid #30363d",fontFamily:"sans-serif",color:"#e6edf3"}}>
      <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",padding:"10px 16px",background:"#161b22",borderBottom:"0.5px solid #30363d"}}>
        <div style={{display:"flex",alignItems:"center",gap:10}}>
          <span style={{fontSize:13,fontWeight:500}}>FlightTracker — SFO</span>
          <span style={{fontSize:11,padding:"2px 8px",borderRadius:20,background:"#0d1f3c",color:"#79c0ff"}}>{total} flights</span>
          <span style={{fontSize:11,padding:"2px 8px",borderRadius:20,background:"#3d1515",color:"#ff7b72"}}>{delayed} delayed</span>
          <span style={{fontSize:11,padding:"2px 8px",borderRadius:20,background:"#1a2a1a",color:"#56d364"}}>● Mock data</span>
        </div>
        <span style={{fontSize:11,color:"#8b949e"}}>Click a plane to inspect</span>
      </div>

      <div style={{position:"relative"}}>
        <svg ref={svgRef} style={{width:"100%",height:480,display:"block"}} />
        <div style={{position:"absolute",top:12,right:12,width:230,background:"#161b22cc",border:"0.5px solid #30363d",borderRadius:8,overflow:"hidden"}}>
          <div style={{padding:"8px 12px",borderBottom:"0.5px solid #30363d",fontSize:11,color:"#8b949e",textTransform:"uppercase",letterSpacing:".07em"}}>Flight detail</div>
          <div style={{padding:"10px 12px"}}>
            {!selectedFlight ? (
              <div style={{fontSize:12,color:"#8b949e"}}>Click a plane to inspect.</div>
            ) : (
              <>
                {[
                  ["Flight", `${selectedFlight.airline_code}${selectedFlight.flight_number}`],
                  ["Route", `${selectedFlight.origin} → ${selectedFlight.destination}`],
                  ["Aircraft", selectedFlight.aircraft_id || "N/A"],
                  ["Gate", selectedFlight.gate_id || "N/A"],
                  ["Status", selectedFlight.status],
                ].map(([l,v]) => (
                  <div key={l} style={{display:"flex",justifyContent:"space-between",fontSize:12,padding:"3px 0",borderBottom:"0.5px solid #21262d"}}>
                    <span style={{color:"#8b949e"}}>{l}</span>
                    <span style={{fontWeight:500}}>{v}</span>
                  </div>
                ))}
                <div style={{display:"flex",justifyContent:"space-between",fontSize:12,padding:"3px 0"}}>
                  <span style={{color:"#8b949e"}}>Delay</span>
                  <span style={{fontWeight:500,color:selectedFlight.delay_minutes>0?"#e3b341":"#56d364"}}>
                    {selectedFlight.delay_minutes > 0 ? `+${selectedFlight.delay_minutes} min` : "On time"}
                  </span>
                </div>
              </>
            )}
          </div>
        </div>
      </div>

      <div style={{display:"flex",flexWrap:"wrap",gap:10,padding:"8px 16px",background:"#161b22",borderTop:"0.5px solid #30363d"}}>
        {[["#56d364","On time"],["#ff7b72","Delayed"],["#79c0ff","Selected"]].map(([col,label]) => (
          <div key={label} style={{display:"flex",alignItems:"center",gap:5,fontSize:10,color:"#8b949e"}}>
            <svg width="10" height="10" viewBox="0 0 10 10"><circle cx="5" cy="5" r="4" fill={col}/></svg>
            {label}
          </div>
        ))}
      </div>
    </div>
  );
}