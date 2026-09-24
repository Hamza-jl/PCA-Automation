"""Build BIA Automation Tool BRD from the provided Word template."""
from docx import Document
from docx.oxml.ns import qn
from docx.oxml import OxmlElement

doc = Document("C:/Users/jelassi.hamza/Downloads/BRD_Template.docx")

# ─── Replace header placeholder ───────────────────────────────────────────────
for para in doc.paragraphs:
    for run in para.runs:
        if "BPMN Smart" in run.text:
            run.text = run.text.replace("BPMN Smart  Plateforme", "BIA Automation Tool")
        if "BPMN" in run.text:
            run.text = run.text.replace("BPMN", "BIA Automation Tool")

# ─── Fill metadata table ──────────────────────────────────────────────────────
t0 = doc.tables[0]
def set_cell(cell, text):
    cell.paragraphs[0].clear()
    cell.paragraphs[0].add_run(text)

set_cell(t0.cell(1, 0), "30/04/2026")
set_cell(t0.cell(1, 1), "Devoteam")
set_cell(t0.cell(1, 2), "1.0")
set_cell(t0.cell(1, 3), "BIA Automation Tool — Business Requirements Document")

# ─── Section content ──────────────────────────────────────────────────────────
SECTIONS = {
    "Executive Summary": [
        ("Normal",
         "The BIA Automation Tool is an internal web application developed for Devoteam's MCO practice. "
         "Its purpose is to eliminate the manual, repetitive work involved in producing Business Impact Analysis (BIA) "
         "documents for clients."),
        ("Normal",
         "Today, consultants spend significant time manually filling in BIA fiches — one Word document per department — "
         "by copying information from a review Excel file into a Word template, then manually consolidating all fiches "
         "into a master Synthèse Excel sheet. This process is slow, error-prone, and must be repeated from scratch for "
         "every new client engagement."),
        ("Normal",
         "The tool replaces this entire process with a simple web interface: the consultant uploads two files, clicks a "
         "button, and receives a complete set of ready-to-deliver BIA documents in seconds. The application also uses "
         "artificial intelligence to automatically detect the client's name and logo from the uploaded files, so no "
         "manual customisation is needed."),
        ("Normal",
         "The MVP has been successfully delivered and validated on a live client project (STAR Assurances)."),
    ],

    "Business Goals & Objectives": [
        ("Normal", "The primary business objectives of the BIA Automation Tool are:"),
        ("List Bullet", "Save time: Reduce the time required to produce a full set of BIA fiches from several hours to under one minute."),
        ("List Bullet", "Improve quality: Ensure every document follows the same structure, naming convention, and branding — eliminating human copy-paste errors."),
        ("List Bullet", "Enable scalability: Allow Devoteam consultants to handle more BIA projects simultaneously without increasing workload."),
        ("List Bullet", "Reduce dependency on individual consultants: Any team member can run the tool without deep knowledge of the BIA document structure."),
        ("List Bullet", "Reusability across clients: The tool works for any Devoteam client without requiring code changes — only the input files change."),
    ],

    "Target Audience": [
        ("Normal", "The tool is designed for the following users within Devoteam:"),
        ("List Bullet", "BIA Project Consultants — the primary users who upload files and download generated documents."),
        ("List Bullet", "Project Managers — who oversee BIA delivery quality and timelines."),
        ("List Bullet", "Practice Leads (MCO) — who may use the tool to review the scope of active BIA engagements."),
        ("Normal",
         "End clients (the organisations being assessed) do not interact with the tool directly. "
         "They receive only the final BIA documents produced by the consultant."),
    ],

    "Scope & Project Boundaries": [
        ("Normal", "In scope:"),
        ("List Bullet", "Automatic generation of one BIA fiche (Word document) per department, populated from an Excel review file and a Word template."),
        ("List Bullet", "Automatic detection of the client name and logo using AI, and injection into all generated documents."),
        ("List Bullet", "Automatic consolidation of all individual BIA fiches into a master Synthèse BIA Excel file."),
        ("List Bullet", "A browser-based interface accessible from any computer on the network — no installation required for end users."),
        ("List Bullet", "Downloadable output: a ZIP archive containing all generated fiches, or a single filled Excel file for the synthèse."),
        ("Normal", "Out of scope:"),
        ("List Bullet", "Editing or reviewing BIA content — the tool does not validate or critique the information provided by the client."),
        ("List Bullet", "User account management, login, or access control (not required for the current internal use case)."),
        ("List Bullet", "Integration with external systems such as SharePoint, ERP, or CRM — planned for a future release."),
        ("List Bullet", "Generating the Synthèse BIA Excel file from scratch — the consultant must always provide the blank template."),
    ],

    "Functional Specifications": [
        ("Normal",
         "The application offers two main workflows, each accessible via a dedicated tab in the web interface."),

        ("Heading 2", "Workflow 1 — Generate BIA Fiches per Department"),
        ("Normal",
         "The consultant uploads two files: the client review Excel file (fiche de recensement des structures) and the blank BIA fiche Word template. "
         "Optionally, they can provide an OpenAI API key to enable AI-powered client detection, and/or enter the client name manually."),
        ("Normal", "Once the files are uploaded and the form is submitted, the tool:"),
        ("List Bullet", "Reads the list of departments and their contacts from the Excel file (department name, vis-à-vis contact, meeting date, organisational level)."),
        ("List Bullet", "Uses AI to automatically identify the client name and logo by analysing the images embedded in the Excel file (if an API key is provided)."),
        ("List Bullet", "Creates one Word document per department, pre-filled with the department name, contact person, meeting date, version number, and full document reference."),
        ("List Bullet", "Replaces all occurrences of the placeholder client name and logo in every generated document."),
        ("List Bullet", "Packages all generated Word files into a single ZIP archive for immediate download."),
        ("Normal",
         "If no API key is provided, the client name defaults to the value entered manually by the consultant, or 'Client' if left blank. "
         "The logo will be extracted automatically from the Excel file without AI assistance."),

        ("Heading 2", "Workflow 2 — Fill the Synthèse BIA"),
        ("Normal",
         "The consultant uploads all completed individual BIA fiches (Word files) along with the blank Synthèse BIA Excel template."),
        ("Normal", "The tool then:"),
        ("List Bullet", "Opens each uploaded fiche and extracts the key BIA data fields: process names, criticality levels, recovery time objectives (RTO), recovery point objectives (RPO), dependencies, and other relevant information."),
        ("List Bullet", "Writes this information into the correct rows and columns of the Synthèse BIA Excel template."),
        ("List Bullet", "Returns the fully populated Synthèse BIA Excel file, ready for client review and delivery."),
        ("Normal",
         "The interface displays a clear summary at the end: how many fiches were successfully processed, and which files (if any) could not be read, along with a plain-language explanation of the issue."),
    ],

    "System Quality Requirements": [
        ("Normal", "The tool must meet the following quality standards:"),
        ("List Bullet",
         "Reliability: Documents must be generated correctly and completely every time, with no missing fields, broken formatting, or incorrect file names."),
        ("List Bullet",
         "Speed: A full batch of up to 30 BIA fiches must be generated in under 60 seconds on a standard office laptop."),
        ("List Bullet",
         "Ease of use: A consultant with no technical background must be able to complete either workflow in under 3 minutes, with no training required beyond reading the on-screen instructions."),
        ("List Bullet",
         "Compatibility: Generated Word files must open correctly in Microsoft Word 2016 and later. Generated Excel files must be compatible with Microsoft Excel 2016 and later."),
        ("List Bullet",
         "Error visibility: If a file cannot be processed, the interface must clearly identify which file failed and provide a plain-language explanation, without stopping the processing of other files."),
        ("List Bullet",
         "Availability: The application should be accessible during standard Devoteam office hours with no planned downtime during business-critical periods."),
    ],

    "Technical Constraints & Risks": [
        ("Normal", "Key constraints and risks to be aware of:"),
        ("List Bullet",
         "Template dependency: The tool is calibrated to the current BIA fiche and Synthèse BIA templates. Any structural change to these templates — such as adding rows, renaming sections, or changing column order — may require a configuration update from the development team."),
        ("List Bullet",
         "AI feature dependency: The automatic client name and logo detection requires an active OpenAI API key and an internet connection. If neither is available, the consultant must enter the client name manually; the logo will be selected automatically as the largest image in the Excel file."),
        ("List Bullet",
         "Excel file format: The review Excel file must follow the expected column structure (Niveau 1 / Niveau 2 / Niveau 3 / Vis-à-vis / Date). Files with a different layout will not be read correctly."),
        ("List Bullet",
         "Server availability: The tool runs on a local or internal server. If the server is offline, the web interface will not be accessible. Deploying the application on a shared internal server is strongly recommended for reliable team-wide access."),
        ("List Bullet",
         "Volume limits: Large review files containing more than 100 departments have not been fully stress-tested in the current MVP version."),
    ],

    "Success Metrics & KPIs": [
        ("Normal", "The following indicators will be used to measure the success of the tool:"),
        ("List Bullet",
         "Time to generate fiches: Target under 1 minute for a batch of up to 30 departments (versus several hours manually)."),
        ("List Bullet",
         "Time to fill the Synthèse: Target under 30 seconds for up to 30 fiches (versus 1 to 2 hours manually)."),
        ("List Bullet",
         "Error rate: Zero formatting, naming, or data errors in generated documents, as verified by the project consultant before delivery."),
        ("List Bullet",
         "User satisfaction: Positive feedback from at least 3 consultants after first use on a real project."),
        ("List Bullet",
         "Adoption rate: Used on at least 3 client BIA projects within 3 months of delivery."),
    ],

    "Implementation Roadmap": [
        ("Normal",
         "The project was delivered in three successive phases over approximately six weeks:"),
        ("List Bullet",
         "Phase 1 (Weeks 1–2) — ETL Core Engine: Built the data extraction logic to read completed BIA fiches and correctly populate the Synthèse Excel template. "
         "Field mapping was validated against the live STAR Assurances templates."),
        ("List Bullet",
         "Phase 2 (Weeks 3–4) — Fiche Generator with AI: Developed the automated fiche generation module, including the AI-powered client name and logo detection feature "
         "using GPT-4o Vision. Validated document quality on real client templates."),
        ("List Bullet",
         "Phase 3 (Weeks 5–6) — Web Interface & Integration: Built the browser-based user interface and connected it to both backend modules via a REST API. "
         "Conducted end-to-end testing with real project files and resolved all identified issues."),
        ("Normal",
         "The MVP was delivered on schedule and successfully validated on a live Devoteam engagement."),
    ],

    "Future Releases": [
        ("Normal", "The following enhancements are planned for future versions of the tool:"),
        ("List Bullet", "Support for additional BIA template versions (v1.0 and any future standard Devoteam templates)."),
        ("List Bullet", "Integration with SharePoint or a shared network drive to automatically save generated documents to the correct project folder."),
        ("List Bullet", "A project history dashboard allowing the team to track which clients have been processed, when, and by whom."),
        ("List Bullet", "Automated email delivery of generated documents to the designated project team members."),
        ("List Bullet", "Support for multi-language BIA templates (French and Arabic)."),
        ("List Bullet", "Role-based access control so only authorised consultants can trigger document generation."),
    ],

    "Conclusion": [
        ("Normal",
         "The BIA Automation Tool addresses a clear and recurring operational pain point within Devoteam's MCO practice. "
         "By automating the most time-consuming steps of the BIA documentation process, it frees consultants to focus on "
         "higher-value analysis and client engagement rather than document formatting."),
        ("Normal",
         "The MVP has proven the core concept and delivered immediate, measurable value on a real client project. "
         "With a clear roadmap for future enhancements, the tool is positioned to become a standard productivity asset "
         "for all Devoteam BIA engagements, benefiting both the practice and the quality of service delivered to clients."),
    ],
}

# ─── Inject content into document ────────────────────────────────────────────
def insert_paragraph_after(anchor_para, style_name, text):
    """Insert a new paragraph immediately after anchor_para.
    anchor_para may be a python-docx Paragraph or a raw CT_P element.
    Returns the raw CT_P of the inserted paragraph.
    """
    new_p = OxmlElement("w:p")
    pPr = OxmlElement("w:pPr")
    pStyle = OxmlElement("w:pStyle")
    style_id_map = {
        "Normal": "Normal",
        "List Bullet": "ListBullet",
        "Heading 2": "Heading2",
    }
    pStyle.set(qn("w:val"), style_id_map.get(style_name, "Normal"))
    pPr.append(pStyle)
    new_p.append(pPr)
    r = OxmlElement("w:r")
    t = OxmlElement("w:t")
    t.text = text
    t.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
    r.append(t)
    new_p.append(r)
    # Support both Paragraph objects and raw CT_P elements
    elem = anchor_para._element if hasattr(anchor_para, "_element") else anchor_para
    elem.addnext(new_p)
    return new_p  # return CT_P so callers can chain

# Build heading → paragraph map
heading_paras = {}
for para in doc.paragraphs:
    if para.style.name == "Heading 1":
        heading_paras[para.text.strip()] = para

# Also try with trailing space variants
for raw_key in list(heading_paras.keys()):
    stripped = raw_key.strip()
    if stripped not in heading_paras:
        heading_paras[stripped] = heading_paras[raw_key]

for section_title, content_list in SECTIONS.items():
    heading_para = None
    for k, v in heading_paras.items():
        if k.strip() == section_title.strip():
            heading_para = v
            break
    if heading_para is None:
        print(f"WARNING: heading not found: {repr(section_title)}")
        continue
    # Insert paragraphs in reverse order, always anchoring after the heading element.
    # Each insertion goes immediately after the heading, so reversed list ends up in correct order.
    heading_elem = heading_para._element
    for style, text in reversed(content_list):
        insert_paragraph_after(heading_elem, style, text)

out = "C:/Users/jelassi.hamza/Desktop/Devoteam/BIA_Implementation/BIA_Automation_BRD.docx"
doc.save(out)
print("Saved:", out)
