export const airports = {
  JFK:{lat:40.64,lon:-73.78},LAX:{lat:33.94,lon:-118.41},ORD:{lat:41.97,lon:-87.91},
  ATL:{lat:33.64,lon:-84.43},BOS:{lat:42.36,lon:-71.01},SFO:{lat:37.62,lon:-122.38},
  DEN:{lat:39.86,lon:-104.67},FLL:{lat:26.07,lon:-80.15},MCO:{lat:28.43,lon:-81.31},
  MIA:{lat:25.79,lon:-80.29},SEA:{lat:47.45,lon:-122.31},IAH:{lat:29.98,lon:-95.34},
  SJU:{lat:18.44,lon:-66.00},LHR:{lat:51.47,lon:-0.46},NRT:{lat:35.77,lon:140.39},
};

export const flights = [
  {id:'AA101',origin:'JFK',dest:'LAX',delay:45,propagated:false,aircraft:'N123AA',gate:'B12',t:0.42},
  {id:'AA205',origin:'JFK',dest:'ORD',delay:34,propagated:true,aircraft:'N123AA',gate:'B14',t:0.55},
  {id:'DL330',origin:'JFK',dest:'ATL',delay:0,propagated:false,aircraft:'N456DL',gate:'C4',t:0.38},
  {id:'DL441',origin:'ATL',dest:'BOS',delay:25,propagated:true,aircraft:'N456DL',gate:'C4',t:0.61},
  {id:'UA567',origin:'JFK',dest:'SFO',delay:0,propagated:false,aircraft:'N789UA',gate:'A2',t:0.47},
  {id:'B6100',origin:'JFK',dest:'FLL',delay:60,propagated:false,aircraft:'N321B6',gate:'T5',t:0.33},
  {id:'B6210',origin:'FLL',dest:'MCO',delay:45,propagated:true,aircraft:'N321B6',gate:'T5',t:0.50},
  {id:'B6310',origin:'MCO',dest:'SJU',delay:33,propagated:true,aircraft:'N321B6',gate:'T5',t:0.44},
  {id:'UA680',origin:'SFO',dest:'DEN',delay:0,propagated:false,aircraft:'N789UA',gate:'A3',t:0.58},
  {id:'AA999',origin:'JFK',dest:'MIA',delay:0,propagated:false,aircraft:'N999AA',gate:'B15',t:0.29},
  {id:'DL007',origin:'JFK',dest:'LHR',delay:0,propagated:false,aircraft:'N007DL',gate:'C8',t:0.52},
  {id:'UA420',origin:'JFK',dest:'NRT',delay:0,propagated:false,aircraft:'N420UA',gate:'A9',t:0.48},
];

export const chains = {
  'B6100':[{id:'B6100',delay:60,reason:'source'},{id:'B6210',delay:45,reason:'aircraft turn'},{id:'B6310',delay:33,reason:'aircraft turn ×2'}],
  'AA101':[{id:'AA101',delay:45,reason:'source'},{id:'AA205',delay:34,reason:'aircraft turn'}],
  'DL330':[{id:'DL330',delay:0,reason:'source'},{id:'DL441',delay:25,reason:'gate reuse'}],
};
