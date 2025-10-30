/* Lightweight helpers (no frameworks) */
(function(){
  // Bottom-sheet Nav: open/close + focus trap + scroll lock
  var btn = document.getElementById('hamburger');
  var sheet = document.getElementById('navsheet');
  var panel = document.getElementById('navpanel');
  var scrim = document.getElementById('navscrim');
  if (btn && sheet && panel){
    var lastActive = null;
    function getFocusables(root){
      return Array.prototype.slice.call(root.querySelectorAll(
        'a[href], button:not([disabled]), input:not([disabled]), textarea, select, [tabindex]:not([tabindex="-1"])'
      ));
    }
    function open(){
      lastActive = document.activeElement;
      sheet.classList.remove('hidden');
      document.body.classList.add('overflow-hidden');
      btn.setAttribute('aria-expanded','true');
      // Animate in: remove offscreen transforms + fade in scrim
      try {
        if (scrim) scrim.classList.remove('opacity-0');
        panel.classList.remove('translate-y-full');
        panel.classList.remove('md:translate-x-full');
      } catch(_){ }
      var f = getFocusables(panel);
      if (f.length) try { f[0].focus(); } catch(_){}
      document.addEventListener('keydown', trap, true);
    }
    function close(){
      // Animate out: add transforms + fade scrim, then hide after transition
      try {
        if (scrim) scrim.classList.add('opacity-0');
        panel.classList.add('translate-y-full');
        panel.classList.add('md:translate-x-full');
      } catch(_){ }
      setTimeout(function(){
        sheet.classList.add('hidden');
        document.body.classList.remove('overflow-hidden');
        btn.setAttribute('aria-expanded','false');
        document.removeEventListener('keydown', trap, true);
        try { if(lastActive) lastActive.focus(); } catch(_){ }
      }, 180);
    }
    function trap(e){
      if (e.key === 'Escape') { e.preventDefault(); close(); return; }
      if (e.key !== 'Tab') return;
      var f = getFocusables(panel); if(!f.length) return;
      var first = f[0], last = f[f.length-1];
      var active = document.activeElement;
      if (e.shiftKey && (active === first || !panel.contains(active))) { e.preventDefault(); last.focus(); }
      else if (!e.shiftKey && (active === last)) { e.preventDefault(); first.focus(); }
    }
    btn.addEventListener('click', function(e){ e.preventDefault(); open(); });
    sheet.addEventListener('click', function(e){ if(e.target && e.target.matches('[data-dismiss], [data-dismiss] *')) close(); });
    // Quick actions
    document.addEventListener('click', function(e){
      var a = e.target && e.target.closest('[data-nav-action]');
      if(!a) return;
      var act = a.getAttribute('data-nav-action');
      if (act === 'search'){
        e.preventDefault(); close(); var q=document.getElementById('q'); if(q) q.focus();
      }
    });
  }

  // ------------------------------------------------------------------
  // Subscribe dialog triggers
  // ------------------------------------------------------------------
  var subscribeDialog = document.getElementById('subscribeDialog');
  if(subscribeDialog){
    document.addEventListener('click', function(e){
      var trg = e.target.closest('[data-open-subscribe]');
      if(!trg) return;
      try { subscribeDialog.showModal(); } catch(_) { subscribeDialog.open = true; }
    });
  }

  // ------------------------------------------------------------------
  // Apply modal trigger & subscribe analytics
  // ------------------------------------------------------------------
  document.addEventListener('click', function(e){
    var el = e.target.closest('[data-apply]');
    if(!el) return;
    e.preventDefault();
    var card = el.closest('[data-job-id]');
    var link = el.getAttribute('data-link') || '';
    var title = el.getAttribute('data-title') || '';
    var payload = jobPayloadFromCard(card, el);
    if(!payload){
      payload = {
        job_id: (card && card.getAttribute('data-job-id')) || '',
        job_title: title,
        company: el.getAttribute('data-company') || '',
        location: el.getAttribute('data-location') || ''
      };
    }
    if (!payload.job_title) payload.job_title = title;
    try {
      window.dispatchEvent(new CustomEvent('open-job-modal', {
        detail: {
          jobId: payload.job_id,
          jobTitle: payload.job_title,
          jobLink: link,
          jobLocation: payload.location,
          jobCompany: payload.company
        }
      }));
    } catch(_) {}
  });

  // ------------------------------------------------------------------
  // Job payload extraction helpers
  // ------------------------------------------------------------------
  function jobPayloadFromCard(card, trigger){
    if(!card) return null;
    var cardDs = card.dataset || {};
    var trigDs = (trigger && trigger.dataset) || {};
    function pick(){
      for (var i = 0; i < arguments.length; i++){
        var val = arguments[i];
        if (typeof val === 'string' && val.trim()){
          return val.trim();
        }
      }
      return '';
    }
    var jobId = pick(trigDs.jobId, cardDs.jobId, card.getAttribute('data-job-id'));
    var title = pick(trigDs.title, trigDs.jobTitle, cardDs.jobTitle);
    if(!title){
      var titleEl = card.querySelector('h2');
      title = titleEl ? (titleEl.textContent || '').trim() : '';
    }
    var company = pick(trigDs.company, cardDs.jobCompany);
    var location = pick(trigDs.location, cardDs.jobLocation);
    if(!company || !location){
      var metaEl = card.querySelector('[data-job-meta]');
      var metaText = metaEl ? (metaEl.textContent || '').trim() : '';
      if(metaText){
        var parts = metaText.split('\u2022');
        var primary = parts[0] ? parts[0].trim() : metaText;
        if(!company && primary.indexOf(' - ') >= 0){
          var seg = primary.split(' - ');
          company = pick(company, seg.shift());
          location = pick(location, seg.join(' - '));
        } else {
          location = pick(location, primary);
        }
      }
    }
    company = company.trim ? company.trim() : company;
    location = location.trim ? location.trim() : location;
    return {
      job_id: jobId,
      job_title: title,
      company: company || '',
      location: location || ''
    };
  }

  try { window.jobPayloadFromCard = jobPayloadFromCard; } catch(_) {}
})();

// --------------------------------------------------------------------
// Job modal (vanilla JS controller)
// --------------------------------------------------------------------
(function(){
  var wrap = document.getElementById('jobModal');
  var dialog = document.getElementById('jobDialog');
  var form = document.getElementById('jobModalForm');
  var emailInput = document.getElementById('jobModalEmail');
  var jobIdField = document.getElementById('jobModalJobId');
  var titleSpan = document.getElementById('jobModalTitle');
  var cancelBtn = document.querySelector('[data-close-job-modal]');
  if (!wrap || !dialog || !form || !emailInput || !jobIdField || !titleSpan) {
    return;
  }

  var jobDetail = {
    jobLink: '',
    jobId: '',
    jobTitle: '',
    jobCompany: '',
    jobLocation: ''
  };

  function hideModal() {
    try { dialog.close(); } catch(_) { dialog.removeAttribute('open'); }
    wrap.classList.add('hidden');
  }

  function showModal() {
    wrap.classList.remove('hidden');
    try { dialog.showModal(); } catch(_) { dialog.setAttribute('open', 'true'); }
    setTimeout(function(){
      try { emailInput.focus(); } catch(_){}
    }, 0);
  }

  window.addEventListener('open-job-modal', function(evt){
    var detail = evt && evt.detail ? evt.detail : {};
    jobDetail.jobLink = detail.jobLink || '';
    jobDetail.jobId = detail.jobId || '';
    jobDetail.jobTitle = detail.jobTitle || '';
    jobDetail.jobCompany = detail.jobCompany || '';
    jobDetail.jobLocation = detail.jobLocation || '';
    jobIdField.value = jobDetail.jobId;
    titleSpan.textContent = jobDetail.jobTitle || 'this role';
    emailInput.value = '';
    showModal();
  });

  wrap.addEventListener('click', function(e){
    if (e.target === wrap) {
      hideModal();
    }
  });

  dialog.addEventListener('cancel', function(e){
    e.preventDefault();
    hideModal();
  });

  dialog.addEventListener('close', function(){
    wrap.classList.add('hidden');
  });

  if (cancelBtn) {
    cancelBtn.addEventListener('click', function(){
      hideModal();
    });
  }

  form.addEventListener('submit', function(e){
    e.preventDefault();
    var email = (emailInput.value || '').trim();
    if (!/.+@.+\..+/.test(email)) {
      emailInput.focus();
      return;
    }
    hideModal();
    var target = jobDetail.jobLink || '';
    if (!target) {
      alert('This job does not have an external apply link yet. Please try another listing.');
      return;
    }
    try {
      var opened = window.open(target, '_blank');
      if (opened) {
        opened.opener = null;
        return;
      }
    } catch(_){}
    try {
      window.location.assign(target);
    } catch(_){
      window.location.href = target;
    }
  });
})();


// --------------------------------------------------------------------
// Search normalization and weekly subscribe toggle
// --------------------------------------------------------------------
(function(){
  var form = document.getElementById('search');
  var q = document.getElementById('q');
  var loc = document.getElementById('loc');
  var toggle = document.getElementById('weekly-toggle');
  var subDlg = document.getElementById('subscribeDialog');
  var COUNTRY_MAP = { de:'DE', deu:'DE', germany:'DE', deutschland:'DE', ch:'CH', schweiz:'CH', suisse:'CH', svizzera:'CH', switzerland:'CH', at:'AT', 'sterreich':'AT', austria:'AT', eu:'EU', europe:'EU', uk:'UK', gb:'UK', england:'UK', 'united kingdom':'UK', us:'US', usa:'US', 'united states':'US', america:'US', es:'ES', spain:'ES', fr:'FR', france:'FR', it:'IT', italy:'IT', nl:'NL', netherlands:'NL', be:'BE', belgium:'BE', se:'SE', sweden:'SE', pl:'PL', poland:'PL', co:'CO', colombia:'CO', mx:'MX', mexico:'MX' };
  var TITLE_MAP = { swe:'software engineer', 'software eng':'software engineer', 'sw eng':'software engineer', frontend:'front end', 'front-end':'front end', backend:'back end', 'back-end':'back end', fullstack:'full stack', 'full-stack':'full stack', pm:'product manager', 'prod mgr':'product manager', 'product owner':'product manager', ds:'data scientist', ml:'machine learning', mle:'machine learning engineer', sre:'site reliability engineer', devops:'devops', 'sec eng':'security engineer', infosec:'security' };
  function normCountry(v){ if(!v) return ''; var t=(v.trim().toLowerCase()); if(COUNTRY_MAP[t]) return COUNTRY_MAP[t]; if(/^[a-z]{2}$/.test(t)) return t.toUpperCase(); return v.trim(); }
  function normTitle(v){ if(!v) return ''; var s=v.toLowerCase(); Object.keys(TITLE_MAP).forEach(function(k){ if(s.indexOf(k)>=0) s=s.replace(new RegExp(k,'g'), TITLE_MAP[k]); }); return s.replace(/\s+/g,' ').trim(); }
  if(form){ form.addEventListener('submit', function(){ if(q) q.value = normTitle(q.value); if(loc) loc.value = normCountry(loc.value); }); }
  if(toggle && subDlg){ toggle.addEventListener('change', function(){ if(toggle.checked){ try{subDlg.showModal();}catch(_) {subDlg.open=true;} var em=document.getElementById('subscribe-email'); if(em) em.focus(); }}); subDlg.addEventListener('close', function(){ toggle.checked=false; }); }
  // Log subscribe dialog native form submission (newsletter)
  }
})();

// Inline script from index.html externalized (kept order and behavior)
(function(){
  try {
    var form = document.getElementById('search');
    var q = document.getElementById('q');
    var loc = document.getElementById('loc');
    if (form) {
      form.addEventListener('submit', function(){
        // Show skeletons during navigation
        try {
          var sk = document.getElementById('results-skeletons');
          if (sk) sk.classList.remove('hidden');
        } catch(_) {}
      });
    }

    // Weekly toggle: support click-outside close for dialog (existing UI)
    var toggle = document.getElementById('weekly-toggle');
    var dlg = document.getElementById('subscribeDialog');
    if (toggle && dlg && dlg.showModal) {
      dlg.addEventListener('click', function(e){
        var r = dlg.getBoundingClientRect();
        if (e.clientX<r.left||e.clientX>r.right||e.clientY<r.top||e.clientY>r.bottom) {
          try { dlg.close(); } catch(_) {}
        }
      });
    }

    // Mobile: advanced search toggle (reveals country input)
    var advBtn = document.getElementById('advanced-toggle');
    var advWrap = document.getElementById('loc-wrap');
    if (advBtn && advWrap) {
      advBtn.addEventListener('click', function(){
        var hidden = advWrap.classList.toggle('hidden');
        advBtn.setAttribute('aria-expanded', (!hidden).toString());
      });
    }

    // Optional details toggle (no-op unless elements exist)
    document.querySelectorAll('[data-toggle="details"]').forEach(function(btn){
      var opened = false;
      btn.addEventListener('click', function(){
        var art = btn.closest('article[data-job-id]');
        var container = art && art.querySelector('[data-details]');
        if (!container) return;
        container.classList.toggle('hidden');
        var expanded = !container.classList.contains('hidden');
        btn.setAttribute('aria-expanded', expanded);
        if (expanded && !opened) {
          opened = true;
        }
      });
    });
  } catch(e) {}
})();


// --------------------------------------------------------------------
// Auto-open job details on scroll
// --------------------------------------------------------------------
(function(){
  if (!('IntersectionObserver' in window)) return;
  var cards = document.querySelectorAll('article[data-job-id]');
  if (!cards.length) return;
  var userScrolled = false;
  var opened = new WeakSet();

  window.addEventListener('scroll', function onScroll(){
    userScrolled = true;
    window.removeEventListener('scroll', onScroll);
  }, { passive: true });

  var observer = new IntersectionObserver(function(entries){
    entries.forEach(function(entry){
      if (!entry.isIntersecting) return;
      if (!userScrolled) return;
      var card = entry.target;
      var details = card.querySelector('details');
      if (!details || details.open) return;
      if (opened.has(details)) return;
      opened.add(details);
      try { details.open = true; } catch(_) {}
    });
  }, { threshold: 0.6 });

  cards.forEach(function(card, idx){
    if (idx === 0) {
      // Keep first card collapsed until user interacts.
      return observer.observe(card);
    }
    observer.observe(card);
  });
})();
