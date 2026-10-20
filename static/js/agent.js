/* ============================================================================
   SmartPCA — l'assistant
   ----------------------------------------------------------------------------
   Une bulle en bas a droite ; au clic, une fenetre de discussion.

   L'assistant repond, mais il agit aussi : il ouvre une page, cherche un
   fichier, ou presente la page courante en surlignant ses elements l'un
   apres l'autre avec un commentaire a cote.

   Les actions arrivent du serveur sous une forme arretee — {type, ...} —
   decidee par du code et non par le modele. Ce fichier ne fait que les
   executer.
   ========================================================================== */
(function () {
  'use strict';
  if (window.__dvAgent) return;
  window.__dvAgent = true;

  var MODELE = 'qwen2.5:3b';
  var ouvert = false;
  var occupe = false;

  /* ── Page courante ────────────────────────────────────────────────────── */
  function pageCourante() {
    var actif = document.querySelector('.app-page.active');
    if (actif && actif.id) return actif.id;
    var p = location.pathname;
    if (p.indexOf('/projects') === 0) return '/projects';
    if (p.indexOf('/fiche-selector') === 0) return '/fiche-selector';
    if (p.indexOf('/fiche-editor') === 0) return '/fiche-selector';
    return 'page-accueil';
  }

  /* ── Construction de l'interface ──────────────────────────────────────── */
  function construire() {
    var bulle = document.createElement('button');
    bulle.className = 'dv-ag-bulle';
    bulle.id = 'dvAgBulle';
    bulle.setAttribute('aria-label', 'Ouvrir l’assistant');
    bulle.innerHTML =
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"' +
      ' stroke-linecap="round" stroke-linejoin="round">' +
      '<path d="M21 11.5a8.4 8.4 0 0 1-9 8.4 8.5 8.5 0 0 1-3.8-.9L3 21l1.9-5.2' +
      'A8.4 8.4 0 0 1 12 3a8.4 8.4 0 0 1 9 8.5z"/></svg>' +
      '<span class="dv-ag-pastille"></span>';

    var pan = document.createElement('div');
    pan.className = 'dv-ag-panneau';
    pan.id = 'dvAgPanneau';
    pan.setAttribute('role', 'dialog');
    pan.setAttribute('aria-label', 'Assistant SmartPCA');
    pan.innerHTML =
      '<div class="dv-ag-tete">' +
        '<img src="/static/devoteam_mark.png" alt="" />' +
        '<div class="dv-ag-tete-txt">' +
          '<strong>Assistant</strong>' +
          '<span id="dvAgContexte">SmartPCA</span>' +
        '</div>' +
        '<button class="dv-ag-fermer" id="dvAgFermer" aria-label="Fermer">' +
          '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4"' +
          ' stroke-linecap="round"><line x1="18" y1="6" x2="6" y2="18"/>' +
          '<line x1="6" y1="6" x2="18" y2="18"/></svg>' +
        '</button>' +
      '</div>' +
      '<div class="dv-ag-fil" id="dvAgFil"></div>' +
      '<div class="dv-ag-suggestions" id="dvAgSuggestions"></div>' +
      '<form class="dv-ag-saisie" id="dvAgForm">' +
        '<input type="text" id="dvAgInput" autocomplete="off" ' +
          'placeholder="Une question, ou « fais-moi une démo »…" />' +
        '<button type="submit" aria-label="Envoyer">' +
          '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4"' +
          ' stroke-linecap="round" stroke-linejoin="round">' +
          '<line x1="22" y1="2" x2="11" y2="13"/>' +
          '<polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>' +
        '</button>' +
      '</form>';

    document.body.appendChild(bulle);
    document.body.appendChild(pan);

    bulle.addEventListener('click', basculer);
    document.getElementById('dvAgFermer').addEventListener('click', basculer);
    document.getElementById('dvAgForm').addEventListener('submit', function (e) {
      e.preventDefault();
      var inp = document.getElementById('dvAgInput');
      var v = inp.value.trim();
      if (!v) return;
      inp.value = '';
      envoyer(v);
    });
  }

  function basculer() {
    ouvert = !ouvert;
    document.getElementById('dvAgPanneau').classList.toggle('open', ouvert);
    document.getElementById('dvAgBulle').classList.toggle('actif', ouvert);
    if (ouvert) {
      majContexte();
      if (!document.getElementById('dvAgFil').children.length) accueillir();
      setTimeout(function () { document.getElementById('dvAgInput').focus(); }, 260);
    }
  }

  function majContexte() {
    var el = document.getElementById('dvAgContexte');
    if (!el) return;
    fetch('/api/agent/page-info?page=' + encodeURIComponent(pageCourante()))
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) { el.textContent = d ? d.titre : 'SmartPCA'; })
      .catch(function () { el.textContent = 'SmartPCA'; });
  }

  /* ── Messages ─────────────────────────────────────────────────────────── */
  function bulleMsg(texte, qui, html) {
    var fil = document.getElementById('dvAgFil');
    var d = document.createElement('div');
    d.className = 'dv-ag-msg ' + qui;
    if (html) d.innerHTML = texte; else d.textContent = texte;
    fil.appendChild(d);
    fil.scrollTop = fil.scrollHeight;
    return d;
  }

  function accueillir() {
    bulleMsg('Je connais l’application et le contenu de votre base. ' +
             'Demandez-moi d’ouvrir une page, de chercher un document, ' +
             'ou de vous présenter la page où vous êtes.', 'ag');
    suggestions([
      'Fais-moi une démo de cette page',
      'À quoi sert cette page ?',
      'Combien de projets dans la base ?',
    ]);
  }

  function suggestions(liste) {
    var z = document.getElementById('dvAgSuggestions');
    z.innerHTML = '';
    liste.forEach(function (s) {
      var b = document.createElement('button');
      b.className = 'dv-ag-sug';
      b.textContent = s;
      b.addEventListener('click', function () { envoyer(s); });
      z.appendChild(b);
    });
  }

  function envoyer(texte) {
    if (occupe) return;
    occupe = true;
    bulleMsg(texte, 'moi');
    document.getElementById('dvAgSuggestions').innerHTML = '';
    var attente = bulleMsg('<span class="dv-ag-points"><i></i><i></i><i></i></span>',
                           'ag', true);

    fetch('/api/agent/chat', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: texte, page: pageCourante(), model: MODELE }),
    })
      .then(function (r) { return r.json(); })
      .then(function (d) {
        attente.remove();
        bulleMsg(d.texte || '—', 'ag');
        if (d.fichiers && d.fichiers.length) listerFichiers(d.fichiers);
        if (d.action) executer(d.action);
      })
      .catch(function (e) {
        attente.remove();
        bulleMsg('Je n’ai pas pu répondre : ' + e.message, 'ag erreur');
      })
      .finally(function () { occupe = false; });
  }

  function listerFichiers(fichiers) {
    var fil = document.getElementById('dvAgFil');
    var z = document.createElement('div');
    z.className = 'dv-ag-fichiers';
    fichiers.forEach(function (f) {
      var a = document.createElement('button');
      a.className = 'dv-ag-fichier';
      a.innerHTML = '<span class="n">' + echapper(f.project_name) + '</span>' +
                    '<span class="m">' + echapper(f.client) + ' · ' +
                    echapper(f.file_type) + '</span>';
      a.addEventListener('click', function () { ouvrirFichier(f); });
      z.appendChild(a);
    });
    fil.appendChild(z);
    fil.scrollTop = fil.scrollHeight;
  }

  function echapper(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  /* ── Actions ──────────────────────────────────────────────────────────── */
  function executer(a) {
    if (!a) return;
    if (a.type === 'naviguer')      allerA(a);
    else if (a.type === 'demo')     lancerDemo(a);
    else if (a.type === 'fermer')   { fermerDemo(); basculer(); }
  }

  function allerA(a) {
    var p = a.page || '';
    if (p.charAt(0) === '/') { window.open(p, '_blank'); return; }
    if (typeof window.dvGo === 'function') {
      window.dvGo(p, a.groupe || '', a.titre || '', null);
      setTimeout(majContexte, 400);
    } else {
      location.href = '/?page=' + encodeURIComponent(p);
    }
  }

  function ouvrirFichier(f) {
    // Les fiches s'ouvrent dans l'editeur, les syntheses se telechargent.
    if (f.file_type === 'synthese') window.open('/api/download/synthese/' + f.id, '_blank');
    else window.open('/fiche-editor?project_id=' + f.id +
                     '&sector=' + encodeURIComponent(f.sector || ''), '_blank');
    bulleMsg('J’ouvre « ' + f.project_name + ' ».', 'ag');
  }

  /* ── Visite guidee ────────────────────────────────────────────────────── */
  var demo = { etapes: [], i: 0, actif: false };

  function lancerDemo(a) {
    var demarrer = function () {
      demo.etapes = a.etapes || [];
      demo.i = 0;
      demo.actif = true;
      if (!demo.etapes.length) { fermerDemo(); return; }
      if (ouvert) basculer();            // la fenetre masquerait la page
      etapeDemo();
    };
    // Si la demo porte sur une autre page, on s'y rend d'abord.
    if (a.page && a.page !== pageCourante() && a.page.charAt(0) !== '/') {
      allerA({ page: a.page, titre: '', groupe: '' });
      setTimeout(demarrer, 650);
    } else {
      demarrer();
    }
  }

  function etapeDemo() {
    fermerCarte();
    if (demo.i >= demo.etapes.length) { fermerDemo(); return; }
    var e = demo.etapes[demo.i];
    var cible = document.querySelector(e.selecteur);

    // Un element absent ne doit pas interrompre la visite : il peut dependre
    // d'une etape que l'utilisateur n'a pas encore faite.
    if (!cible || !cible.offsetParent) { demo.i++; return etapeDemo(); }

    cible.scrollIntoView({ block: 'center', behavior: 'smooth' });
    setTimeout(function () { placerCarte(cible, e); }, 340);
  }

  function placerCarte(cible, e) {
    var r = cible.getBoundingClientRect();

    var trou = document.createElement('div');
    trou.className = 'dv-ag-surlignage';
    trou.id = 'dvAgSurlignage';
    trou.style.top = (r.top - 6) + 'px';
    trou.style.left = (r.left - 6) + 'px';
    trou.style.width = (r.width + 12) + 'px';
    trou.style.height = (r.height + 12) + 'px';
    document.body.appendChild(trou);

    var carte = document.createElement('div');
    carte.className = 'dv-ag-carte';
    carte.id = 'dvAgCarte';
    carte.innerHTML =
      '<div class="dv-ag-carte-num">' + (demo.i + 1) + ' / ' + demo.etapes.length + '</div>' +
      '<h4>' + echapper(e.titre) + '</h4>' +
      '<p>' + echapper(e.texte) + '</p>' +
      '<div class="dv-ag-carte-btns">' +
        '<button class="dv-ag-skip" id="dvAgStop">Terminer</button>' +
        '<button class="dv-ag-next" id="dvAgNext">' +
          (demo.i === demo.etapes.length - 1 ? 'Fin' : 'Suivant') +
        '</button>' +
      '</div>';
    document.body.appendChild(carte);

    // A droite si la place le permet, sinon a gauche, sinon dessous.
    var L = r.right + 18, T = r.top;
    var lc = carte.offsetWidth, hc = carte.offsetHeight;
    if (L + lc > window.innerWidth - 12) L = r.left - lc - 18;
    if (L < 12) { L = Math.max(12, r.left); T = r.bottom + 14; }
    if (T + hc > window.innerHeight - 12) T = Math.max(12, window.innerHeight - hc - 12);
    carte.style.left = L + 'px';
    carte.style.top = T + 'px';

    document.getElementById('dvAgNext').addEventListener('click', function () {
      demo.i++; etapeDemo();
    });
    document.getElementById('dvAgStop').addEventListener('click', fermerDemo);
  }

  function fermerCarte() {
    ['dvAgSurlignage', 'dvAgCarte'].forEach(function (id) {
      var el = document.getElementById(id);
      if (el) el.remove();
    });
  }

  function fermerDemo() {
    fermerCarte();
    if (demo.actif) {
      demo.actif = false;
      if (!ouvert) basculer();
      bulleMsg('Visite terminée. Autre chose ?', 'ag');
    }
  }

  document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape') { if (demo.actif) fermerDemo(); else if (ouvert) basculer(); }
  });
  window.addEventListener('resize', function () { if (demo.actif) etapeDemo(); });

  /* ── Demarrage ────────────────────────────────────────────────────────── */
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', construire);
  } else {
    construire();
  }
})();
