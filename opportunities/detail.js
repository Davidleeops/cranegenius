(function(){
  var FORM_ENDPOINT = 'https://formspree.io/f/mgoldjjb';
  var currentOpportunity = null;

  function byId(id){ return document.getElementById(id); }
  function esc(s){ return String(s||'').replace(/[&<>\"]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c];}); }

  function renderOpportunity(op){
    currentOpportunity = op;
    byId('name').textContent = op.project_name;
    byId('loc').textContent = [op.project_address, op.city + ', ' + op.state, op.country].filter(Boolean).join(' • ');
    byId('stageBadge').textContent = (op.project_stage || 'planning').replace(/_/g,' ').toUpperCase();

    var rows = [
      ['Project Type', op.project_type],
      ['Industry Segment', op.industry_segment],
      ['Developer', op.developer_optional || 'Not listed'],
      ['General Contractor', op.general_contractor_optional || 'Not listed'],
      ['Estimated Start', op.estimated_start_date_optional || 'TBD'],
      ['Estimated Completion', op.estimated_completion_date_optional || 'TBD'],
      ['Data Source', op.data_source],
      ['Confidence', op.confidence_score_optional || 'N/A']
    ];

    byId('overview').innerHTML = rows.map(function(r){
      return '<div><div class="k">'+esc(r[0])+'</div><div class="v">'+esc(r[1])+'</div></div>';
    }).join('');

    var useCases = (op.inferred_use_cases_optional||[]).length ? (' Use cases: ' + op.inferred_use_cases_optional.join(', ') + '.') : '';
    byId('summary').textContent = op.lift_requirements_summary + useCases;
    byId('cats').innerHTML = (op.recommended_lift_categories||[]).map(function(c){ return '<span class="tag">'+esc(c)+'</span>'; }).join('');
  }

  function applyCrossLinks(op){
    var cats = op.recommended_lift_categories || [];
    var firstCategory = encodeURIComponent(cats[0] || 'specialty_lift_contractors');
    document.querySelectorAll('a[href=\"/marketplace/\"]').forEach(function(a){
      a.href = '/marketplace/?lift=' + firstCategory;
    });
    document.querySelectorAll('a[href=\"/data-centers/ai-planner\"]').forEach(function(a){
      a.href = '/data-centers/ai-planner?ref=opportunity_engine';
    });
  }

  function fetchOpportunity(){
    window.CGOpportunityDataLoader.loadOpportunityDataset()
      .then(function(result){
        var slug = window.CG_OPPORTUNITY_SLUG || '';
        var list = result && result.opportunities ? result.opportunities : [];
        var op = list.find(function(x){ return x.project_slug === slug; });
        if(!op){ byId('name').textContent='Opportunity Not Found'; byId('loc').textContent='This project slug is not available.'; return; }
        renderOpportunity(op);
        applyCrossLinks(op);
      })
      .catch(function(){
        byId('name').textContent='Opportunity Load Error';
        byId('loc').textContent='Could not load opportunity data.';
      });
  }

  window.openOppModal = function(defaultType){
    if(defaultType){ byId('oppRequestType').value = defaultType; }
    byId('oppModalWrap').style.display = 'flex';
  };
  window.closeOppModal = function(){ byId('oppModalWrap').style.display = 'none'; };

  window.submitOppInquiry = function(){
    if(!currentOpportunity){ return; }
    var name = byId('oppName').value.trim();
    var company = byId('oppCompany').value.trim();
    var email = byId('oppEmail').value.trim();
    var phone = byId('oppPhone').value.trim();
    var requestType = byId('oppRequestType').value;
    var notes = byId('oppNotes').value.trim();
    var msg = byId('oppMsg');
    if(!name || !email){ msg.style.color='#ff6b6b'; msg.textContent='Name and email are required.'; return; }

    // Unified payload pattern aligned with marketplace/planner/estimator.
    var payload = window.CGOpportunityHelpers.buildUnifiedOpportunityLeadPayload({
      lead_type: 'opportunity_inquiry',
      source_page: window.location.pathname,
      source_tool: 'opportunity_engine',
      opportunity_id: currentOpportunity.opportunity_id,
      project_name: currentOpportunity.project_name,
      request_type: requestType,
      selected_lift_category: (currentOpportunity.recommended_lift_categories||[])[0] || 'specialty_lift_contractors',
      recommended_solution_type: (currentOpportunity.recommended_lift_categories||[]).join(' | '),
      contact_name: name,
      company_name: company,
      email: email,
      phone: phone,
      notes: notes,
      project_location: [currentOpportunity.city,currentOpportunity.state,currentOpportunity.country].filter(Boolean).join(', '),
      project_type: currentOpportunity.project_type,
      equipment_context: currentOpportunity.lift_requirements_summary,
      _subject: 'Opportunity Inquiry — ' + currentOpportunity.project_name
    }, {
      // Backward compatibility aliases.
      name: name,
      company: company,
      project_slug: currentOpportunity.project_slug
    });

    msg.style.color='#9ec2ff'; msg.textContent='Submitting...';
    fetch(FORM_ENDPOINT, {
      method:'POST',
      headers:{'Content-Type':'application/json','Accept':'application/json'},
      body:JSON.stringify(payload)
    }).then(function(r){
      if(!r.ok) throw new Error('submit_failed');
      msg.style.color='#00C4A7'; msg.textContent='Request sent. CraneGenius will follow up soon.';
      setTimeout(function(){ closeOppModal(); }, 900);
    }).catch(function(){
      msg.style.color='#ff6b6b'; msg.textContent='Submission failed. Email info@cranegenius.com or call 503-773-4659.';
    });
  };

  fetchOpportunity();
})();
