import React, { useState } from 'react';
import LiveMap from './pages/LiveMap.jsx';
import FindParking from './pages/FindParking.jsx';
import CarparkDetail from './pages/CarparkDetail.jsx';
import Insights from './pages/Insights.jsx';
import Model from './pages/Model.jsx';
import Pipeline from './pages/Pipeline.jsx';
import Attribution from './components/Attribution.jsx';

const TABS = ['Live Map', 'Find Parking', 'Carpark Detail', 'Insights', 'Model', 'Pipeline'];

const SLUG = Object.fromEntries(TABS.map((t) => [t.toLowerCase().replace(/ /g, '-'), t]));
// deep links: #carpark/AM20 or #insights, #model, ...
function fromHash() {
  const [page, id] = window.location.hash.replace(/^#\/?/, '').split('/');
  if (page === 'carpark' && id) return { tab: 'Carpark Detail', id: decodeURIComponent(id) };
  return { tab: SLUG[page] || 'Live Map', id: null };
}

export default function App() {
  const init = fromHash();
  const [tab, setTabState] = useState(init.tab);
  const [carpark, setCarparkState] = useState(init.id || 'AM20');
  const setTab = (t) => { setTabState(t); window.location.hash = t === 'Carpark Detail' ? `carpark/${carpark}` : t.toLowerCase().replace(/ /g, '-'); };
  const setCarpark = (id) => { setCarparkState(id); window.location.hash = `carpark/${id}`; };
  const open = (id) => { setCarparkState(id); setTabState('Carpark Detail'); window.location.hash = `carpark/${id}`; };
  return (
    <div className="app">
      <header>
        <div className="brand"><h1>ParkSense SG</h1><span>Will the carpark be full when you arrive?</span></div>
        <nav>{TABS.map((t) => <button key={t} className={t === tab ? 'on' : ''} onClick={() => setTab(t)}>{t}</button>)}</nav>
      </header>
      <main>
        {tab === 'Live Map' && <LiveMap onOpen={open} />}
        {tab === 'Find Parking' && <FindParking onOpen={open} />}
        {tab === 'Carpark Detail' && <CarparkDetail id={carpark} setId={setCarpark} />}
        {tab === 'Insights' && <Insights onOpen={open} />}
        {tab === 'Model' && <Model />}
        {tab === 'Pipeline' && <Pipeline />}
      </main>
      <Attribution />
    </div>
  );
}
