import { useEffect, useRef, useState, useCallback } from "react";
import * as d3 from "d3";
import * as topojson from "topojson-client";

const airports = {
  JFK:{lat:40.64,lon:-73.78},LAX:{lat:33.94,lon:-118.41},ORD:{lat:41.97,lon:-87.91},
  ATL:{lat:33.64,lon:-84.43},BOS:{lat:42.36,lon:-71.01},SFO:{lat:37.62,lon:-122.38},
  DEN:{lat:39.86,lon:-104.67},FLL:{lat:26.07,lon:-80.15},MCO:{lat:28.43,lon:-81.31},
  MIA:{lat:25.79,lon:-80.29},SEA:{lat:47.45,lon:-122.31},IAH:{lat:29.98,lon:-95.34},
  SJU:{lat:18.44,lon:-66.00},LHR:{lat:51.47,lon:-0.46},NRT:{lat:35.77,lon:140.39},
};

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

function flightPosition(f) {
  if (!f.scheduled_departure || !f.scheduled_arrival) return 0.45;
  const dep = new Date(f.scheduled_departure).getTime();
  const arr = new Date(f.scheduled_arrival).getTime();
  const now = Date.now();
  const t = (now - dep) / (arr - dep);
  return Math.max(0.05, Math.min(0.95, t));
}

export default function FlightMap() {
  const svgRef = useRef(null);
  const [selected, setSelected] = useState(null);
  const [worldData, setWorldData] = useState(null);
  const [flights, setFlights] = useState([]);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef(null);

  // load world map once
  useEffect(() => {
    fetch("https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json")
      .then(r => r.json())
      .then(setWorldData);
  }, []);

  // websocket connection
  useEffect(() => {
    const ws = new WebSocket("ws://localhost:8000/ws/JFK");
    wsRef.current = ws;

    ws.onopen = () => setConnected(true);
    ws.onclose = () => setConnected(false);

    ws.onmessage = (event) => {
      const newFlight = JSON.parse(event.data);
      setFlights(prev => {
        const idx = prev.findIndex(f => f.flight_key === newFlight.flight_key);
        if (idx >= 0) {
          const updated = [...prev];
          updated[idx] = newFlight;
          return updated;
        }
        return [...prev, newFlight];
      });
    };

    return () => ws.close();
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
      const a = airports[code];
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
      routeG.append("path")
        .attr("d", `M${p1[0]},${p1[1]} Q${ctrl[0]},${ctrl[1]} ${p2[0]},${p2[1]}`)
        .attr("fill", "none")
        .attr("stroke", f.delay_minutes > 0 ? "#ff7b72" : "#1e3050")
        .attr("stroke-width", f.delay_minutes > 0 ? 1.5 : 0.7)
        .attr("stroke-opacity", f.delay_minutes > 0 ? 0.65 : 0.25);
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
        .attr("font-size",8).attr("fill","#8b949e").text(code);
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
        .append("path").attr("d", "M0,-7 L4,4 L0,2 L-4,4 Z")
        .attr("fill", col).attr("stroke", "#0d1117").attr("stroke-width", 0.8);

      if (f.delay_minutes > 0) {
        g.append("circle").attr("cx",7).attr("cy",-7).attr("r",4)
          .attr("fill","#ff7b72").attr("stroke","#0d1117").attr("stroke-width",0.5);
        g.append("text").attr("x",7).attr("y",-4)
          .attr("text-anchor","middle").attr("font-size",5).attr("font-weight",700)
          .attr("fill","#0d1117").text("!");
      }
    });

  }, [worldData, flights, selected]);

  const selectedFlight = flights.find(f => f.flight_key === selected);
  const delayed = flights.filter(f => f.delay_minutes > 0).length;

  return (
    <div style={{background:"#0d1117",borderRadius:12,overflow:"hidden",border:"0.5px solid #30363d",fontFamily:"sans-serif",color:"#e6edf3"}}>
      <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",padding:"10px 16px",background:"#161b22",borderBottom:"0.5px solid #30363d"}}>
        <div style={{display:"flex",alignItems:"center",gap:10}}>
          <span style={{fontSize:13,fontWeight:500}}>FlightTracker — JFK</span>
          <span style={{fontSize:11,padding:"2px 8px",borderRadius:20,background:"#3d1515",color:"#ff7b72"}}>{delayed} delayed</span>
          <span style={{fontSize:11,padding:"2px 8px",borderRadius:20,background:connected?"#0d2818":"#2a2a2a",color:connected?"#56d364":"#8b949e"}}>
            {connected ? "● Live" : "○ Connecting..."}
          </span>
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