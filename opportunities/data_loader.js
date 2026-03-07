(function(global){
  var CANDIDATE_DATASET_PATHS = [
    '/data/assets/opportunities.json',
    '/data/assets/opportunity_dataset.json',
    '/data/opportunities_batch_2.json',
    '/data/opportunities/imported_opportunities.json',
    '/data/opportunities/opportunities_repo.json',
    '/data/opportunities/opportunities.json',
    '/data/opportunities/seed_opportunities.json'
  ];

  function toSlug(v){
    return String(v||'').toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/^-+|-+$/g,'');
  }

  function normalizeOpportunity(o, idx){
    var n = Object.assign({}, o || {});
    n.opportunity_id = n.opportunity_id || ('opp-auto-' + (idx + 1));
    n.project_name = n.project_name || ('Opportunity ' + (idx + 1));
    n.project_slug = n.project_slug || toSlug(n.project_name) || ('opportunity-' + (idx + 1));
    n.city = n.city || '';
    n.state = n.state || '';
    n.country = n.country || 'USA';
    n.project_type = n.project_type || 'infrastructure_upgrade';
    n.project_stage = n.project_stage || 'planning';
    n.status = n.status || 'new';
    n.recommended_lift_categories = Array.isArray(n.recommended_lift_categories)
      ? n.recommended_lift_categories
      : (global.CGOpportunityHelpers ? global.CGOpportunityHelpers.inferLiftCategories(n.project_type) : []);
    return n;
  }

  function extractOpportunityArray(data){
    if (!data) return [];
    if (Array.isArray(data)) return data;
    if (Array.isArray(data.opportunities)) return data.opportunities;
    if (Array.isArray(data.items)) return data.items;
    return [];
  }

  async function loadOpportunityDataset(){
    var merged = [];
    var sourcePaths = [];
    for (var i = 0; i < CANDIDATE_DATASET_PATHS.length; i++) {
      var path = CANDIDATE_DATASET_PATHS[i];
      try {
        var res = await fetch(path, { cache: 'no-store' });
        if (!res.ok) continue;
        var json = await res.json();
        var list = extractOpportunityArray(json);
        if (!list.length) continue;
        sourcePaths.push(path);
        merged = merged.concat(list);
      } catch (e) {}
    }
    if (!merged.length) {
      return {
        source_path: 'inline_empty_fallback',
        source_paths: [],
        used_seed_fallback: true,
        opportunities: []
      };
    }
    var seen = {};
    var normalized = [];
    merged.map(normalizeOpportunity).forEach(function(x){
      var k = x.opportunity_id || x.project_slug;
      if (!k || seen[k]) return;
      seen[k] = true;
      normalized.push(x);
    });
    return {
      source_path: sourcePaths[0],
      source_paths: sourcePaths,
      used_seed_fallback: sourcePaths.some(function(p){ return /seed_opportunities\.json$/.test(p); }),
      opportunities: normalized
    };
  }

  global.CGOpportunityDataLoader = {
    CANDIDATE_DATASET_PATHS: CANDIDATE_DATASET_PATHS,
    loadOpportunityDataset: loadOpportunityDataset
  };
})(window);
