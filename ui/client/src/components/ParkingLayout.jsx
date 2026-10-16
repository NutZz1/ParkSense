import React, { useEffect, useMemo, useState } from 'react';
import { buildLevels, templateOf, TEMPLATE_NAMES } from '../layout.js';

const FREE = '#2e9e4f', TAKEN = '#d64545';
const W = 30, H = 48, GAP = 4, AISLE = 34;

// Top-down grey car (architectural-plan style: light body, darker windscreen/rear window, mirrors, soft shadow).
function CarDefs() {
  return (
    <defs>
      <filter id="carShadow" x="-20%" y="-20%" width="140%" height="140%">
        <feDropShadow dx="0" dy="0.8" stdDeviation="0.9" floodColor="#000" floodOpacity="0.28" />
      </filter>
      <linearGradient id="carBody" x1="0" x2="1">
        <stop offset="0" stopColor="#b9babf" /><stop offset="0.5" stopColor="#d0d1d5" /><stop offset="1" stopColor="#b9babf" />
      </linearGradient>
      <symbol id="car" viewBox="0 0 30 48">
        <g filter="url(#carShadow)">
          {/* mirrors */}
          <path d="M5.2 15.2 L2.6 14.4 L2.6 16.4 L5.2 16.8 Z" fill="#9fa1a6" />
          <path d="M24.8 15.2 L27.4 14.4 L27.4 16.4 L24.8 16.8 Z" fill="#9fa1a6" />
          {/* body */}
          <rect x="5" y="4" width="20" height="40" rx="7.5" fill="url(#carBody)" stroke="#a3a5aa" strokeWidth="0.5" />
          {/* windscreen, roof, rear window */}
          <path d="M7.6 14.5 Q15 11 22.4 14.5 L21.2 20 Q15 18.6 8.8 20 Z" fill="#8c8e93" />
          <rect x="8.8" y="20.6" width="12.4" height="11.4" rx="1.8" fill="#c8c9cd" />
          <path d="M8.9 32.6 Q15 34 21.1 32.6 L22 37.4 Q15 40.2 8 37.4 Z" fill="#8c8e93" />
          {/* side windows */}
          <path d="M7.4 21 L8.3 21 L8.3 31.6 L7.4 31 Z" fill="#9b9da2" /><path d="M22.6 21 L21.7 21 L21.7 31.6 L22.6 31 Z" fill="#9b9da2" />
          {/* lights */}
          <rect x="7.5" y="4.6" width="3.2" height="1.2" rx="0.6" fill="#e9e6c9" /><rect x="19.3" y="4.6" width="3.2" height="1.2" rx="0.6" fill="#e9e6c9" />
          <rect x="7.5" y="42.3" width="3.2" height="1" rx="0.5" fill="#c97b7b" /><rect x="19.3" y="42.3" width="3.2" height="1" rx="0.5" fill="#c97b7b" />
        </g>
      </symbol>
    </defs>
  );
}

// Bay = rectangle tinted green (available) or red (occupied, with a car), separated by thin lines.
function Slot({ x, y, taken, label }) {
  return (
    <g>
      <rect x={x} y={y} width={W} height={H} rx={1.5} fill={taken ? '#fbe2e2' : '#e1f4e6'}
        stroke={taken ? TAKEN : FREE} strokeWidth={1.1}>
        <title>{`${label}: ${taken ? 'occupied' : 'available'}`}</title>
      </rect>
      {taken
        ? <use href="#car" x={x + 1} y={y + 1} width={W - 2} height={H - 2} pointerEvents="none" />
        : <text x={x + W / 2} y={y + H / 2 + 4} textAnchor="middle" fontSize={10} fill={FREE} fontWeight="600" pointerEvents="none">{label.split(' ')[1]}</text>}
    </g>
  );
}

// Bays in pairs of rows facing an aisle (like a real deck); cols bays per row.
function LotSvg({ slots, cols }) {
  const rows = Math.ceil(slots.length / cols);
  const bands = Math.ceil(rows / 2);
  const bandH = 2 * (H + GAP) + AISLE;
  const width = cols * (W + GAP) + GAP;
  const height = Math.max(1, bands) * bandH;
  return (
    <svg className="lot" viewBox={`0 0 ${width} ${height}`} width="100%" role="img" aria-label="parking slots">
      <CarDefs />
      {Array.from({ length: bands }, (_, b) => (
        <g key={`a${b}`}>
          <rect x={0} y={b * bandH + 2 * (H + GAP)} width={width} height={AISLE - GAP} fill="#f3f4f6" />
          <line x1={8} x2={width - 8} y1={b * bandH + 2 * (H + GAP) + (AISLE - GAP) / 2} y2={b * bandH + 2 * (H + GAP) + (AISLE - GAP) / 2}
            stroke="#c5cad1" strokeWidth={1.5} strokeDasharray="10 8" />
        </g>
      ))}
      {slots.map((taken, i) => {
        const r = Math.floor(i / cols), c = i % cols;
        const x = GAP + c * (W + GAP), y = Math.floor(r / 2) * bandH + (r % 2) * (H + GAP);
        return <Slot key={i} x={x} y={y} taken={taken} label={`Slot ${i + 1}`} />;
      })}
    </svg>
  );
}

// Roadside: bays along the kerb on both sides of a street, 15 per side per segment.
function RoadsideSvg({ slots }) {
  const per = 15, seg = Math.ceil(slots.length / (2 * per));
  const RW = 46, segH = 2 * H + RW + 24, width = per * (W + GAP) + GAP;
  return (
    <svg className="lot" viewBox={`0 0 ${width} ${Math.max(1, seg) * segH}`} width="100%" role="img" aria-label="roadside bays">
      <CarDefs />
      {Array.from({ length: seg }, (_, s) => (
        <g key={s}>
          <rect x={0} y={s * segH + H + 4} width={width} height={RW} fill="#59636c" />
          <line x1={0} x2={width} y1={s * segH + H + 4 + RW / 2} y2={s * segH + H + 4 + RW / 2} stroke="#f3d34a" strokeWidth={2} strokeDasharray="14 10" />
        </g>
      ))}
      {slots.map((taken, i) => {
        const s = Math.floor(i / (2 * per)), k = i % (2 * per), side = k < per ? 0 : 1, c = k % per;
        const x = GAP + c * (W + GAP), y = s * segH + (side === 0 ? 0 : H + 8 + RW);
        return <Slot key={i} x={x} y={y} taken={taken} label={`Bay ${i + 1}`} />;
      })}
    </svg>
  );
}

export default function ParkingLayout({ carpark }) {
  const tpl = templateOf(carpark);
  const levels = useMemo(() => buildLevels(carpark), [carpark.id, carpark.total, carpark.available]);
  const [active, setActive] = useState(0);
  useEffect(() => setActive(0), [carpark.id]);
  const lv = levels[Math.min(active, levels.length - 1)];
  const cols = tpl === 'surface' ? 20 : 10;
  return (
    <div className="parking">
      <div className="parking-head">
        <span className="tpl">{TEMPLATE_NAMES[tpl]}</span>
        {levels.length > 1 && (
          <div className="levels" role="tablist">
            {levels.map((l, i) => (
              <button key={l.name} role="tab" className={i === active ? 'on' : ''} onClick={() => setActive(i)}
                title={`${l.free} free of ${l.size}`}>
                {l.name}<small>{l.free}</small>
              </button>
            ))}
          </div>
        )}
      </div>
      <div className="level-summary">
        <b>{lv.name}</b>: {lv.free} available · {lv.occupied} occupied · {lv.size} slots
        <span className="legend"><i style={{ background: '#e1f4e6', border: `2px solid ${FREE}` }} /> available <i style={{ background: '#fbe2e2', border: `2px solid ${TAKEN}` }} /> occupied (car)</span>
      </div>
      <div className="lot-wrap">{tpl === 'roadside' ? <RoadsideSvg slots={lv.slots} /> : <LotSvg slots={lv.slots} cols={cols} />}</div>
      <p className="note">
        Live totals ({carpark.available} free of {carpark.total}) come from HDB. The feed reports totals only, so which
        slots are shown red is illustrative{levels.length > 1 ? ' (occupied slots fill from the lowest level up)' : ''}.
        {(tpl === 'multistorey' || tpl === 'basement') && ` Levels show 100 slots each; the real building has ${carpark.decks ?? '–'} deck(s).`}
        {tpl === 'roadside' && ' Roadside is a display style for small carparks (under 60 lots); HDB does not publish on-street parking.'}
      </p>
    </div>
  );
}
