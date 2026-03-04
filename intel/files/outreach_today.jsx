import { useState } from "react";

const leads = [
  {
    rank: 1,
    name: "Blattner Energy — Project Coordinator",
    type: "Solar EPC",
    why: "Largest utility-scale solar EPC in North America. Actively building multiple 100-500MW projects in TX and NV right now. They need cranes for transformer installation and substation construction on EVERY project and almost never get calls from crane companies.",
    phone: "320-251-9490",
    ask: "Hi, I work with a network of crane operators across Texas and Nevada and I specialize in utility-scale solar and industrial projects. I'm calling because Blattner has several active projects in our service area and I wanted to connect with whoever manages your heavy lift subcontractors for transformer and substation work.",
    location: "Avon, MN (projects in TX/NV)",
    potential: "$$$",
    timeline: "Immediate — active builds",
    color: "#dc2626",
  },
  {
    rank: 2,
    name: "ExxonMobil Baytown Complex — Maintenance Dept",
    type: "Refinery Turnaround",
    why: "One of the largest refinery complexes in North America. Baytown runs major turnarounds every 4-5 years on a rotating unit basis — meaning there's ALWAYS something coming up. Their turnaround team is separate from operations and rarely gets cold outreach.",
    phone: "281-834-1000",
    ask: "Hi, I'm reaching out to connect with your turnaround maintenance planning team. We provide crane and rigging services specifically for planned turnarounds and I wanted to get on your approved vendor list before your next planned outage.",
    location: "Baytown, TX",
    potential: "$$$",
    timeline: "6-18 months — research current TAR schedule",
    color: "#dc2626",
  },
  {
    rank: 3,
    name: "Primoris Services Corp — Heavy Civil Division",
    type: "Solar + Industrial EPC",
    why: "Dallas-based EPC doing massive solar and industrial construction across TX/NV. Publicly traded so you can look up their active project backlog on EDGAR before calling. They just reported $2.8B in solar backlog.",
    phone: "214-740-5600",
    ask: "I saw your Q4 filing — congratulations on the solar backlog growth. I specialize in crane services for EPC firms on utility-scale projects and wanted to connect with your team handling heavy lift subcontractor selection for the Texas and Nevada builds.",
    location: "Dallas, TX",
    potential: "$$$",
    timeline: "Immediate — active pipeline",
    color: "#dc2626",
  },
  {
    rank: 4,
    name: "Panasonic Energy (TRIC Gigafactory) — Facilities/Construction",
    type: "Industrial Facility",
    why: "Panasonic's EV battery gigafactory at Tahoe Reno Industrial Center is one of the largest industrial projects in Nevada history. Phase 2 construction is ongoing and Erick is 4 hours away. This is the single best lead to walk in with when you meet Erick.",
    phone: "775-856-4000 (Reno regional)",
    ask: "I work with crane operators in the northern Nevada market and I know Panasonic has ongoing construction and equipment installation at the TRIC campus. I wanted to connect with whoever manages your heavy equipment and rigging contractors.",
    location: "Storey County, NV (TRIC)",
    potential: "$$$",
    timeline: "Active — ongoing Phase 2",
    color: "#dc2626",
  },
  {
    rank: 5,
    name: "Coeur Mining — Rochester Mine",
    type: "Nevada Mining",
    why: "Just completed a $680M expansion at their Rochester silver mine in Pershing County, NV. Post-expansion, they now have ongoing maintenance cycles for crushers, conveyors, and mill equipment that require crane work on a recurring basis. Mine maintenance supers almost never get crane vendor outreach.",
    phone: "312-489-5800",
    ask: "I specialize in crane services for mining operations in Nevada — specifically equipment maintenance, conveyor replacements, and crusher installations. I know Rochester just completed a major expansion and I wanted to connect with your maintenance team about upcoming equipment work.",
    location: "Pershing County, NV",
    potential: "$$",
    timeline: "Ongoing — recurring maintenance",
    color: "#ea580c",
  },
  {
    rank: 6,
    name: "Shell Deer Park Refinery — Turnaround Planning",
    type: "Refinery Turnaround",
    why: "Shell Deer Park is one of the most active refinery complexes in the Houston Ship Channel for turnarounds. They have a dedicated TAR planning team you can reach directly. Major units rotate every 3-5 years.",
    phone: "281-884-3000",
    ask: "I'm reaching out to your turnaround planning team. We provide crane and rigging services for planned refinery maintenance events and I wanted to introduce ourselves before your next scheduled TAR. Can you connect me with whoever manages your crane subcontractors?",
    location: "Deer Park, TX",
    potential: "$$$",
    timeline: "Research current TAR schedule",
    color: "#ea580c",
  },
  {
    rank: 7,
    name: "Switch Data Centers — Construction/Facilities",
    type: "Data Center",
    why: "Switch operates a massive data center campus at TRIC (Tahoe Reno Industrial Center). They are in ongoing expansion with new modules being built. Data center construction requires cranes for cooling unit installation, generator placement, and UPS systems. Nobody is calling Switch about cranes.",
    phone: "702-444-4000",
    ask: "I work with crane companies in northern Nevada and I know Switch has been expanding the TRIC campus. I specialize in data center construction crane services — cooling equipment, generator installation, electrical infrastructure. I wanted to connect with your construction team.",
    location: "Storey County, NV (TRIC)",
    potential: "$$",
    timeline: "Active expansion",
    color: "#ea580c",
  },
  {
    rank: 8,
    name: "Lithium Nevada Corp (Thacker Pass) — Construction Manager",
    type: "Mining / Processing Plant",
    why: "Thacker Pass lithium mine in Humboldt County is one of the largest new mining construction projects in North America right now. The processing facility is under construction with a major EPC contractor. This is crane work on a massive scale and almost nobody in crane sales has called the EPC.",
    phone: "775-623-3811 (Winnemucca area)",
    ask: "I know Thacker Pass processing facility construction is underway and I specialize in crane services for large industrial construction in Nevada. I wanted to connect with your construction management team or the EPC contractor handling the processing plant.",
    location: "Humboldt County, NV",
    potential: "$$$",
    timeline: "Active construction 2025-2026",
    color: "#dc2626",
  },
  {
    rank: 9,
    name: "LyondellBasell Houston Complex — Maintenance Engineering",
    type: "Petrochemical Turnaround",
    why: "One of the largest petrochemical complexes in North America with multiple units on rotating TAR schedules. Their maintenance engineering team manages crane subcontractors directly. The complex is massive enough that there's nearly always a unit in some phase of planning or execution.",
    phone: "713-309-7200",
    ask: "I provide crane and rigging services for planned turnarounds and maintenance events at petrochemical facilities. I work with operators on the Houston Ship Channel and wanted to get connected with your maintenance engineering team about upcoming planned outages.",
    location: "Houston, TX",
    potential: "$$$",
    timeline: "Research active TAR schedule",
    color: "#ea580c",
  },
  {
    rank: 10,
    name: "Mortenson Construction — Renewable Energy Division",
    type: "Solar/Wind EPC",
    why: "Mortenson is one of the top renewable energy EPC contractors in the US with active projects in NV and TX. Their renewable division manages crane subcontractors centrally. A single relationship here could unlock 5-10 active projects per year.",
    phone: "763-522-2100",
    ask: "I specialize in crane services for utility-scale renewable energy construction — specifically transformer installation, substation work, and heavy lift for solar and wind projects. Mortenson has projects in our Nevada and Texas service areas and I wanted to connect with your crane subcontractor team.",
    location: "Minneapolis, MN (projects in TX/NV)",
    potential: "$$$",
    timeline: "Multiple active projects",
    color: "#ea580c",
  },
];

const sourceColors = {
  "Solar EPC": "#f59e0b",
  "Refinery Turnaround": "#ef4444",
  "Industrial Facility": "#8b5cf6",
  "Nevada Mining": "#10b981",
  "Data Center": "#3b82f6",
  "Mining / Processing Plant": "#10b981",
  "Petrochemical Turnaround": "#ef4444",
  "Solar/Wind EPC": "#f59e0b",
};

export default function OutreachList() {
  const [expanded, setExpanded] = useState(null);
  const [called, setCalled] = useState({});

  const toggleCalled = (i) => {
    setCalled((prev) => ({ ...prev, [i]: !prev[i] }));
  };

  return (
    <div style={{ fontFamily: "'Inter', sans-serif", background: "#0f172a", minHeight: "100vh", padding: "24px" }}>
      <div style={{ maxWidth: 800, margin: "0 auto" }}>
        {/* Header */}
        <div style={{ marginBottom: 32 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 8 }}>
            <div style={{ background: "#f59e0b", borderRadius: 8, padding: "6px 12px", fontSize: 12, fontWeight: 700, color: "#000", letterSpacing: 1 }}>
              CRANEGENIUS
            </div>
            <div style={{ color: "#64748b", fontSize: 13 }}>
              {new Date().toLocaleDateString("en-US", { weekday: "long", month: "long", day: "numeric" })}
            </div>
          </div>
          <h1 style={{ color: "#f1f5f9", fontSize: 26, fontWeight: 700, margin: "0 0 6px 0" }}>
            10 Calls To Make Today
          </h1>
          <p style={{ color: "#94a3b8", fontSize: 14, margin: 0 }}>
            Ranked by deal potential. Click any card for the exact script and context.
          </p>
          <div style={{ display: "flex", gap: 16, marginTop: 16 }}>
            <div style={{ background: "#1e293b", borderRadius: 8, padding: "8px 16px", color: "#f1f5f9", fontSize: 13 }}>
              <span style={{ color: "#f59e0b", fontWeight: 700 }}>{Object.values(called).filter(Boolean).length}</span>
              <span style={{ color: "#64748b" }}> / 10 called</span>
            </div>
            <div style={{ background: "#1e293b", borderRadius: 8, padding: "8px 16px", color: "#94a3b8", fontSize: 13 }}>
              Tap a card → expand script
            </div>
          </div>
        </div>

        {/* Lead Cards */}
        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          {leads.map((lead, i) => {
            const isExpanded = expanded === i;
            const isDone = called[i];
            const tagColor = sourceColors[lead.type] || "#64748b";

            return (
              <div
                key={i}
                onClick={() => setExpanded(isExpanded ? null : i)}
                style={{
                  background: isDone ? "#0f1a12" : "#1e293b",
                  border: `1px solid ${isDone ? "#166534" : isExpanded ? "#f59e0b" : "#334155"}`,
                  borderRadius: 12,
                  padding: 20,
                  cursor: "pointer",
                  transition: "all 0.15s",
                  opacity: isDone ? 0.6 : 1,
                }}
              >
                {/* Card Header */}
                <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 12 }}>
                  <div style={{ display: "flex", alignItems: "flex-start", gap: 14, flex: 1 }}>
                    {/* Rank badge */}
                    <div style={{
                      minWidth: 36,
                      height: 36,
                      borderRadius: 8,
                      background: isDone ? "#166534" : lead.color,
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                      fontWeight: 800,
                      fontSize: 16,
                      color: "#fff",
                    }}>
                      {isDone ? "✓" : lead.rank}
                    </div>

                    <div style={{ flex: 1 }}>
                      <div style={{ color: "#f1f5f9", fontWeight: 600, fontSize: 15, lineHeight: 1.3 }}>
                        {lead.name}
                      </div>
                      <div style={{ display: "flex", gap: 8, marginTop: 6, flexWrap: "wrap" }}>
                        <span style={{ background: tagColor + "22", color: tagColor, fontSize: 11, fontWeight: 600, borderRadius: 4, padding: "2px 8px" }}>
                          {lead.type}
                        </span>
                        <span style={{ color: "#64748b", fontSize: 12 }}>📍 {lead.location}</span>
                        <span style={{ color: lead.potential === "$$$" ? "#f59e0b" : "#94a3b8", fontSize: 12, fontWeight: 700 }}>
                          {lead.potential}
                        </span>
                      </div>
                    </div>
                  </div>

                  <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                    <div style={{ color: "#64748b", fontSize: 18 }}>
                      {isExpanded ? "▲" : "▼"}
                    </div>
                  </div>
                </div>

                {/* Expanded content */}
                {isExpanded && (
                  <div style={{ marginTop: 20, borderTop: "1px solid #334155", paddingTop: 20 }}>
                    {/* Why this lead */}
                    <div style={{ marginBottom: 16 }}>
                      <div style={{ color: "#94a3b8", fontSize: 11, fontWeight: 700, letterSpacing: 1, marginBottom: 6 }}>
                        WHY THIS LEAD
                      </div>
                      <div style={{ color: "#cbd5e1", fontSize: 14, lineHeight: 1.6 }}>
                        {lead.why}
                      </div>
                    </div>

                    {/* Timeline */}
                    <div style={{ marginBottom: 16 }}>
                      <div style={{ color: "#94a3b8", fontSize: 11, fontWeight: 700, letterSpacing: 1, marginBottom: 6 }}>
                        TIMELINE
                      </div>
                      <div style={{ color: "#f59e0b", fontSize: 13, fontWeight: 600 }}>
                        ⏱ {lead.timeline}
                      </div>
                    </div>

                    {/* Script */}
                    <div style={{ marginBottom: 20 }}>
                      <div style={{ color: "#94a3b8", fontSize: 11, fontWeight: 700, letterSpacing: 1, marginBottom: 8 }}>
                        CALL SCRIPT (say this)
                      </div>
                      <div style={{
                        background: "#0f172a",
                        border: "1px solid #334155",
                        borderRadius: 8,
                        padding: 14,
                        color: "#e2e8f0",
                        fontSize: 14,
                        lineHeight: 1.7,
                        fontStyle: "italic",
                      }}>
                        "{lead.ask}"
                      </div>
                    </div>

                    {/* Actions */}
                    <div style={{ display: "flex", gap: 10 }}>
                      <a
                        href={`tel:${lead.phone.replace(/[^0-9]/g, "")}`}
                        onClick={(e) => e.stopPropagation()}
                        style={{
                          background: "#f59e0b",
                          color: "#000",
                          fontWeight: 700,
                          fontSize: 13,
                          padding: "10px 20px",
                          borderRadius: 8,
                          textDecoration: "none",
                          display: "flex",
                          alignItems: "center",
                          gap: 6,
                        }}
                      >
                        📞 {lead.phone}
                      </a>
                      <button
                        onClick={(e) => { e.stopPropagation(); toggleCalled(i); }}
                        style={{
                          background: isDone ? "#166534" : "#1e293b",
                          color: isDone ? "#4ade80" : "#94a3b8",
                          border: `1px solid ${isDone ? "#166534" : "#475569"}`,
                          fontWeight: 600,
                          fontSize: 13,
                          padding: "10px 20px",
                          borderRadius: 8,
                          cursor: "pointer",
                        }}
                      >
                        {isDone ? "✓ Called" : "Mark Called"}
                      </button>
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>

        {/* Footer */}
        <div style={{ marginTop: 32, padding: 20, background: "#1e293b", borderRadius: 12, border: "1px solid #334155" }}>
          <div style={{ color: "#f59e0b", fontWeight: 700, fontSize: 13, marginBottom: 8 }}>
            🏗️ FOR ERICK MEETING — TOP 3 NEVADA PLAYS
          </div>
          <div style={{ color: "#94a3b8", fontSize: 13, lineHeight: 1.8 }}>
            1. Panasonic TRIC (#4) — Active Phase 2 construction, 4 hours away, ongoing crane need<br/>
            2. Lithium Nevada Thacker Pass (#8) — Massive new construction, get on EPC bid list now<br/>
            3. Switch Data Centers TRIC (#7) — Ongoing modular expansion, nobody is calling about cranes
          </div>
          <div style={{ color: "#475569", fontSize: 12, marginTop: 12 }}>
            Pull Storey + Washoe County permit data before the meeting → show $3B+ of active construction Erick's team isn't calling.
          </div>
        </div>
      </div>
    </div>
  );
}
