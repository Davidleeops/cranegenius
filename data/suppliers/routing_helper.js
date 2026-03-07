(function(global){
  var REGION_BY_STATE_OR_PROVINCE = {
    WA: "west", OR: "west", CA: "west", AZ: "west", NV: "west", UT: "west", CO: "west", ID: "west", NM: "west", MT: "west", WY: "west",
    TX: "south", FL: "south", GA: "south", NC: "south", SC: "south", AL: "south", TN: "south", LA: "south", OK: "south", AR: "south", VA: "south",
    IL: "midwest", OH: "midwest", MI: "midwest", WI: "midwest", IN: "midwest", MO: "midwest", IA: "midwest", MN: "midwest", KS: "midwest", NE: "midwest", ND: "midwest", SD: "midwest",
    NY: "northeast", NJ: "northeast", PA: "northeast", MA: "northeast", CT: "northeast", RI: "northeast", NH: "northeast", VT: "northeast", ME: "northeast", DE: "northeast", MD: "northeast",
    ON: "canada_central", QC: "canada_east", BC: "canada_west", AB: "canada_west", MB: "canada_central", SK: "canada_central", NS: "canada_east", NB: "canada_east"
  };

  var ROUTING_STATUS_VALUES = ["suggested", "reviewed", "routed", "dismissed"];

  function nowIso(){ return new Date().toISOString(); }
  function toSlug(v){ return String(v || "").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, ""); }
  function ensureArray(v){
    if (Array.isArray(v)) return v.filter(Boolean);
    if (typeof v === "string" && v.trim()) return [v.trim()];
    return [];
  }
  function overlap(a, b){
    var bs = new Set(ensureArray(b));
    return ensureArray(a).filter(function(x){ return bs.has(x); });
  }
  function getRegionCode(stateOrProvince){
    return REGION_BY_STATE_OR_PROVINCE[String(stateOrProvince || "").toUpperCase()] || "";
  }
  function opportunityCategories(opportunity){
    return ensureArray(opportunity && opportunity.recommended_lift_categories);
  }

  function computeMatch(opportunity, supplier){
    var score = 0;
    var reasons = [];
    var oppState = String(opportunity.state || "").toUpperCase();
    var oppRegion = getRegionCode(oppState);
    var serviceRegions = ensureArray(supplier.service_regions).map(function(x){ return String(x).toLowerCase(); });
    var matchedCategories = overlap(opportunityCategories(opportunity), supplier.equipment_categories);
    var specialtyOverlap = overlap([opportunity.project_type, opportunity.industry_segment], supplier.specialties_optional);

    if (!matchedCategories.length) {
      return { score: 0, matched_categories: [], reasons: ["No lift-category overlap"] };
    }

    if (String(supplier.state_or_province || "").toUpperCase() === oppState && oppState) {
      score += 40;
      reasons.push("Exact state/province match");
    } else if (oppRegion && serviceRegions.indexOf(oppRegion) >= 0) {
      score += 25;
      reasons.push("Service region overlap");
    } else if (serviceRegions.indexOf("national") >= 0) {
      score += 15;
      reasons.push("National fallback coverage");
    }

    score += Math.min(35, matchedCategories.length * 12);
    reasons.push("Equipment category overlap: " + matchedCategories.join(", "));

    if (specialtyOverlap.length) {
      score += 10;
      reasons.push("Specialty/project overlap");
    }

    return {
      score: Math.max(0, Math.min(100, Math.round(score))),
      matched_categories: matchedCategories,
      reasons: reasons
    };
  }

  function buildRoutingResult(opportunity, supplier, matchMeta, status){
    var routingStatus = ROUTING_STATUS_VALUES.indexOf(status) >= 0 ? status : "suggested";
    var sourceId = opportunity.opportunity_id || toSlug(opportunity.project_name) || "opp-unknown";
    return {
      routing_id: "route-" + sourceId + "-" + supplier.supplier_id,
      source_type: "opportunity",
      source_id: sourceId,
      source_slug_optional: opportunity.project_slug || "",
      supplier_id: supplier.supplier_id,
      supplier_name: supplier.company_name,
      routing_score: matchMeta.score,
      routing_reason: (matchMeta.reasons || []).join(" • "),
      matched_categories: matchMeta.matched_categories || [],
      created_timestamp: nowIso(),
      routing_status: routingStatus
    };
  }

  function buildRoutingLeadObject(opportunity, supplier, routingResult){
    return {
      lead_id: "lead-" + routingResult.routing_id,
      source_type: "opportunity",
      source_id: routingResult.source_id,
      supplier_id: supplier.supplier_id,
      supplier_name: supplier.company_name,
      project_name: opportunity.project_name || "",
      city: opportunity.city || "",
      state: opportunity.state || "",
      recommended_lift_categories: opportunityCategories(opportunity),
      routing_score: routingResult.routing_score,
      lead_status: "new",
      created_timestamp: nowIso()
    };
  }

  // Main function requested for this sprint.
  function matchSuppliersToOpportunity(opportunity, suppliers, options){
    var maxResults = (options && options.max_results) || 5;
    var supplierList = Array.isArray(suppliers) ? suppliers : (global.CGSupplierRoutingEngine._lastSuppliers || []);
    var ranked = (supplierList || []).filter(function(s){ return s && s.active_status !== false; }).map(function(supplier){
      var matchMeta = computeMatch(opportunity, supplier);
      var routing = buildRoutingResult(opportunity, supplier, matchMeta, "suggested");
      return {
        routing: routing,
        supplier: supplier,
        matched_categories: matchMeta.matched_categories || [],
        routing_score: routing.routing_score,
        routing_reason: routing.routing_reason,
        lead_object: buildRoutingLeadObject(opportunity, supplier, routing)
      };
    }).filter(function(row){
      return row.routing_score > 0;
    }).sort(function(a, b){
      return b.routing_score - a.routing_score;
    }).slice(0, maxResults);
    return ranked;
  }

  function matchAllOpportunities(opportunities, suppliers, options){
    global.CGSupplierRoutingEngine._lastSuppliers = suppliers || [];
    var out = [];
    (opportunities || []).forEach(function(op){
      matchSuppliersToOpportunity(op, suppliers, options).forEach(function(match){
        out.push({
          opportunity: op,
          supplier: match.supplier,
          routing: match.routing,
          lead_object: match.lead_object
        });
      });
    });
    return out;
  }

  // Future placeholders:
  // - supplier notification engine
  // - agent claim workflow
  // - CRM sync
  // - success fee tracking/audit trail
  global.CGSupplierRoutingEngine = {
    ROUTING_STATUS_VALUES: ROUTING_STATUS_VALUES,
    matchSuppliersToOpportunity: matchSuppliersToOpportunity,
    matchAllOpportunities: matchAllOpportunities,
    buildRoutingLeadObject: buildRoutingLeadObject,
    _lastSuppliers: []
  };
})(window);
