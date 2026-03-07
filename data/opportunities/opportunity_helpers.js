(function(global){
  // First-pass mapping only. Future scraper/classifier agents should enrich this logic.
  var PROJECT_TYPE_TO_LIFT_CATEGORIES = {
    data_center_construction: ["mobile_cranes","compact_lifting_equipment","rigging_and_lifting_accessories"],
    hospital_construction: ["tower_cranes","mobile_cranes"],
    commercial_tower: ["tower_cranes","mobile_cranes","glass_handling_systems"],
    glass_facade_project: ["glass_handling_systems","vacuum_lifts","spider_cranes"],
    industrial_equipment_installation: ["heavy_equipment_installation_lifts","rigging_and_lifting_accessories","compact_lifting_equipment"],
    manufacturing_expansion: ["mobile_cranes","industrial_lifting_systems","rigging_and_lifting_accessories"],
    logistics_distribution_center: ["mobile_cranes","crawler_cranes","specialty_lift_contractors"],
    infrastructure_upgrade: ["crawler_cranes","mobile_cranes","heavy_equipment_installation_lifts"]
  };

  function inferLiftCategories(projectType, existing) {
    if (Array.isArray(existing) && existing.length) return existing;
    return PROJECT_TYPE_TO_LIFT_CATEGORIES[projectType] || ["specialty_lift_contractors"];
  }

  // Unified lead object shared with marketplace/planner/estimator.
  // Future CRM ingestion can hook into this shape directly.
  function buildUnifiedOpportunityLeadPayload(base, extra){
    return Object.assign({
      lead_type: "opportunity_inquiry",
      source_page: window.location.pathname || "/opportunities/",
      source_tool: "opportunity_engine",
      opportunity_id: "",
      project_name: "",
      request_type: "quote_request",
      selected_lift_category: "",
      recommended_solution_type: "",
      created_timestamp: new Date().toISOString(),
      status_placeholder: "new",
      contact_name: "",
      company_name: "",
      email: "",
      phone: "",
      notes: "",
      project_location: "",
      project_type: "",
      equipment_context: "",
      load_weight: "",
      lift_height: "",
      radius: "",
      indoor_or_outdoor: "",
      access_constraints: ""
    }, base || {}, extra || {});
  }

  global.CGOpportunityHelpers = {
    PROJECT_TYPE_TO_LIFT_CATEGORIES: PROJECT_TYPE_TO_LIFT_CATEGORIES,
    inferLiftCategories: inferLiftCategories,
    buildUnifiedOpportunityLeadPayload: buildUnifiedOpportunityLeadPayload
  };
})(window);
