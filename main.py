from __future__ import annotations

import asyncio
import json
import os
import re
from copy import deepcopy
from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Any
from xml.sax.saxutils import escape
from zipfile import ZIP_DEFLATED, ZipFile

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
import uvicorn
import xml.etree.ElementTree as ET

APP_DIR = Path(__file__).resolve().parent
STATE_FILE = APP_DIR / "project_state.json"
EXPORT_DIR = APP_DIR / "exports"
UPLOAD_DIR = APP_DIR / "uploads"
PRIMAVERA_EXPORT_FILE = EXPORT_DIR / "primavera_schedule_import.xlsx"
TODAY = date.today()
PRIMAVERA_PROJECT_ID = "BOQIMPORT"
XML_NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

SPECIALIST_AGENTS = [
    {"id": "agent-1", "agent_name": "WBS_Extractor_01", "wbs_category": "Doors and Partitions", "sequence": 1, "task": "Extract BOQ scope for doors, glazing, drywall partitions, and metal partitions into scheduler-ready activities.", "keywords": ["door", "doors", "partition", "drywall", "gypsum partition", "glass partition", "ironmongery", "frame", "shutter"], "resource_list": "Doors Crew", "template_output": [{"WBS": "Doors and Partitions", "Activity Name": "Partitions - Marking"}, {"WBS": "Doors and Partitions", "Activity Name": "Partitions - Framing"}, {"WBS": "Doors and Partitions", "Activity Name": "Doors - Frame Installation"}, {"WBS": "Doors and Partitions", "Activity Name": "Doors - Shutter Hanging"}]},
    {"id": "agent-2", "agent_name": "WBS_Extractor_02", "wbs_category": "Wood Works", "sequence": 2, "task": "Extract joinery, cabinetry, counters, cladding, and carpentry items into fabrication and installation steps.", "keywords": ["wood", "joinery", "cabinet", "counter", "countertop", "vanity", "carpentry", "timber", "mdf", "veneer"], "resource_list": "Joinery Crew", "template_output": [{"WBS": "Wood Works", "Activity Name": "Joinery - Site Measurements"}, {"WBS": "Wood Works", "Activity Name": "Joinery - Fabrication"}, {"WBS": "Wood Works", "Activity Name": "Joinery - Finishing"}, {"WBS": "Wood Works", "Activity Name": "Joinery - Installation"}]},
    {"id": "agent-3", "agent_name": "WBS_Extractor_03", "wbs_category": "Ceiling", "sequence": 3, "task": "Extract suspended ceilings, gypsum ceilings, acoustic systems, and ceiling paint into staged activities.", "keywords": ["ceiling", "suspended", "acoustic", "gypsum ceiling", "soffit"], "resource_list": "Ceiling Crew", "template_output": [{"WBS": "Ceiling", "Activity Name": "Ceiling - Framing"}, {"WBS": "Ceiling", "Activity Name": "Ceiling - Board Fixing"}, {"WBS": "Ceiling", "Activity Name": "Ceiling - Putty"}, {"WBS": "Ceiling", "Activity Name": "Ceiling - Paint Final Coat"}]},
    {"id": "agent-4", "agent_name": "WBS_Extractor_04", "wbs_category": "Floor Finishes", "sequence": 4, "task": "Extract flooring systems such as tile, stone, vinyl, carpet, and raised floor into preparation and installation activities.", "keywords": ["floor", "tile", "tiles", "carpet", "vinyl", "raised floor", "epoxy", "marble", "granite", "skirting"], "resource_list": "Flooring Crew", "template_output": [{"WBS": "Floor Finishes", "Activity Name": "Floor Finishes - Layout"}, {"WBS": "Floor Finishes", "Activity Name": "Ceramic Tiles - Setting"}, {"WBS": "Floor Finishes", "Activity Name": "Ceramic Tiles - Grouting"}, {"WBS": "Floor Finishes", "Activity Name": "Raised Floor - Installation"}]},
    {"id": "agent-5", "agent_name": "WBS_Extractor_05", "wbs_category": "Wall Finishes", "sequence": 5, "task": "Extract wall finishes such as paint, wall covering, cladding, and plastering into buildable steps.", "keywords": ["wall", "paint", "painting", "plaster", "putty", "wallpaper", "cladding", "render", "skim coat"], "resource_list": "Wall Finishes Crew", "template_output": [{"WBS": "Wall Finishes", "Activity Name": "Wall Finishes - Putty"}, {"WBS": "Wall Finishes", "Activity Name": "Paint - First Coat"}, {"WBS": "Wall Finishes", "Activity Name": "Paint - Second Coat"}, {"WBS": "Wall Finishes", "Activity Name": "Paint - Final Coat"}]},
    {"id": "agent-6", "agent_name": "WBS_Extractor_06", "wbs_category": "HVAC", "sequence": 6, "task": "Extract ducting, air-side equipment, insulation, and diffusers into sequenced HVAC activities.", "keywords": ["hvac", "duct", "ducting", "ahu", "fcu", "vrf", "ventilation", "diffuser", "grille", "thermostat"], "resource_list": "HVAC Crew", "template_output": [{"WBS": "HVAC", "Activity Name": "Ducting - Fabrication"}, {"WBS": "HVAC", "Activity Name": "Ducting - First Fix"}, {"WBS": "HVAC", "Activity Name": "Ducting - Insulation"}, {"WBS": "HVAC", "Activity Name": "FCU - Final Installation"}]},
    {"id": "agent-7", "agent_name": "WBS_Extractor_07", "wbs_category": "Electrical", "sequence": 7, "task": "Extract power, lighting, fire alarm, data, and small power systems into first-fix and second-fix activities.", "keywords": ["electrical", "lighting", "light", "power", "socket", "switch", "fire alarm", "data", "cable", "conduit", "panel"], "resource_list": "Electrical Crew", "template_output": [{"WBS": "Electrical", "Activity Name": "Lighting - Conduit First Fix"}, {"WBS": "Electrical", "Activity Name": "Lighting - Wiring First Fix"}, {"WBS": "Electrical", "Activity Name": "Power - Socket Installation"}, {"WBS": "Electrical", "Activity Name": "Fire Alarm - Control Panel Installation"}]},
    {"id": "agent-8", "agent_name": "WBS_Extractor_08", "wbs_category": "Miscellaneous", "sequence": 8, "task": "Extract waterproofing, testing, signage, specialties, and close coordination items into actionable activities.", "keywords": ["waterproof", "signage", "testing", "specialty", "accessory", "toilet accessory", "mirror", "handrail", "membrane"], "resource_list": "Specialties Crew", "template_output": [{"WBS": "Miscellaneous", "Activity Name": "Waterproofing - Membrane Installation"}, {"WBS": "Miscellaneous", "Activity Name": "Signage - Installation"}, {"WBS": "Miscellaneous", "Activity Name": "Testing - Plumbing Leak Check"}, {"WBS": "Miscellaneous", "Activity Name": "Branding - Accessories Fixing"}]},
    {"id": "agent-9", "agent_name": "WBS_Extractor_09", "wbs_category": "Outdoor Areas", "sequence": 9, "task": "Extract external works, landlord interfaces, approvals, and site access scope into schedulable activities.", "keywords": ["outdoor", "external", "paving", "landlord", "approval", "permit", "facade", "sitework", "landscape", "access"], "resource_list": "External Works Crew", "template_output": [{"WBS": "Outdoor Areas", "Activity Name": "Landlord Approval - Shop Drawings"}, {"WBS": "Outdoor Areas", "Activity Name": "External Paving - Installation"}, {"WBS": "Outdoor Areas", "Activity Name": "Access Coordination - Permit Clearance"}, {"WBS": "Outdoor Areas", "Activity Name": "Landlord Signoff - Final Walkthrough"}]},
]

PLANNER_AGENT = {"id": "agent-10", "name": "Project Manager Agent", "role": "PMP construction planner", "goal": "Collect the specialist agent outputs, sequence package activities, and generate a Primavera import workbook that follows the uploaded sample structure.", "flow": ["Read uploaded BOQ", "Run specialist extractors in parallel", "Normalize package activities", "Build concurrent package schedule", "Generate Primavera TASK/TASKPRED workbook"]}

DURATION_RULES = {"Doors and Partitions": [2, 4, 3, 2], "Wood Works": [2, 5, 3, 3], "Ceiling": [3, 3, 2, 2], "Floor Finishes": [2, 4, 2, 3], "Wall Finishes": [2, 2, 2, 2], "HVAC": [4, 5, 3, 3], "Electrical": [3, 4, 3, 2], "Miscellaneous": [2, 2, 2, 2], "Outdoor Areas": [2, 4, 2, 2]}
PACKAGE_WBS_CODES = {"Preliminaries": "BOQIMPORT.PRELIM", "Doors and Partitions": "BOQIMPORT.ARCH.DoorsPartitions", "Wood Works": "BOQIMPORT.ARCH.WoodWorks", "Ceiling": "BOQIMPORT.ARCH.Ceiling", "Floor Finishes": "BOQIMPORT.ARCH.FloorFinishes", "Wall Finishes": "BOQIMPORT.ARCH.WallFinishes", "HVAC": "BOQIMPORT.MEP.HVAC", "Electrical": "BOQIMPORT.MEP.Electrical", "Miscellaneous": "BOQIMPORT.Specialties", "Outdoor Areas": "BOQIMPORT.External", "Closeout / Testing & Commissioning": "BOQIMPORT.Closeout"}

class ChatRequest(BaseModel):
    message: str

class TimelineEventRequest(BaseModel):
    date: str | None = None
    reason: str
    lost_days: int = 1


def seed_state() -> dict[str, Any]:
    agents = []
    for agent in SPECIALIST_AGENTS:
        agent_state = deepcopy(agent)
        agent_state["status"] = "ready"
        agent_state["boq_matches"] = 0
        agent_state["latest_output"] = deepcopy(agent["template_output"])
        agent_state["last_run"] = None
        agents.append(agent_state)
    schedule = build_schedule(agents, [])
    return {"agents": agents, "planner": {**deepcopy(PLANNER_AGENT), "status": "ready", "last_run": None, "export_file": PRIMAVERA_EXPORT_FILE.name, "export_updated_at": None}, "workflow": {"status": "idle", "last_run": None, "mode": "parallel-specialists-then-project-manager"}, "boq_upload": {"filename": None, "stored_path": None, "uploaded_at": None, "status": "No BOQ uploaded yet", "row_count": 0, "detected_sheet": None}, "timeline": {"start_date": TODAY.isoformat(), "finish_date": schedule[-1]["finish_date"], "schedule": schedule, "events": []}, "chat_history": [{"role": "assistant", "content": "Upload a BOQ workbook, then run the workflow to launch all specialist agents together and generate the Primavera export."}], "project_summary": {"total_duration_days": sum(item["duration_days"] for item in schedule), "delay_events": 0, "last_action": "Dashboard initialized", "primavera_rows": len(schedule)}}


def load_state() -> dict[str, Any]:
    if not STATE_FILE.exists():
        state = seed_state()
        save_state(state)
        return state
    state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    seeded = seed_state()
    state.setdefault("agents", seeded["agents"])
    for seeded_agent in seeded["agents"]:
        existing = next((agent for agent in state["agents"] if agent.get("id") == seeded_agent["id"]), None)
        if not existing:
            state["agents"].append(seeded_agent)
            continue
        for key in ["agent_name", "wbs_category", "sequence", "task", "keywords", "resource_list", "template_output"]:
            existing[key] = deepcopy(seeded_agent[key])
        existing.setdefault("status", "ready")
        existing.setdefault("boq_matches", 0)
        existing.setdefault("latest_output", deepcopy(seeded_agent["template_output"]))
        existing.setdefault("last_run", None)
    state.setdefault("planner", seeded["planner"])
    state["planner"]["id"] = PLANNER_AGENT["id"]
    state["planner"]["name"] = PLANNER_AGENT["name"]
    state["planner"]["role"] = PLANNER_AGENT["role"]
    state["planner"]["goal"] = PLANNER_AGENT["goal"]
    state["planner"]["flow"] = deepcopy(PLANNER_AGENT["flow"])
    state["planner"].setdefault("status", "ready")
    state["planner"].setdefault("last_run", None)
    state["planner"]["export_file"] = PRIMAVERA_EXPORT_FILE.name
    state["planner"].setdefault("export_updated_at", None)
    state.setdefault("workflow", seeded["workflow"])
    state["workflow"].setdefault("status", "idle")
    state["workflow"].setdefault("last_run", None)
    state["workflow"]["mode"] = "parallel-specialists-then-project-manager"
    state.setdefault("boq_upload", seeded["boq_upload"])
    state["boq_upload"].setdefault("row_count", 0)
    state["boq_upload"].setdefault("detected_sheet", None)
    state.setdefault("timeline", seeded["timeline"])
    state["timeline"].setdefault("events", [])
    state.setdefault("chat_history", seeded["chat_history"])
    state.setdefault("project_summary", seeded["project_summary"])
    recalculate_timeline(state, state["project_summary"].get("last_action", "State loaded"))
    save_state(state)
    return state


def save_state(state: dict[str, Any]) -> None:
    recalculate_timeline(state, state["project_summary"].get("last_action", "State saved"))
    refresh_primavera_export(state)
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def append_chat(state: dict[str, Any], role: str, content: str) -> None:
    state["chat_history"].append({"role": role, "content": content})


def excel_column_name(index: int) -> str:
    label = ""
    current = index
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        label = chr(65 + remainder) + label
    return label

def xml_cell(cell_ref: str, value: Any) -> str:
    if value is None or value == "":
        return f'<c r="{cell_ref}"/>'
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f'<c r="{cell_ref}"><v>{value}</v></c>'
    return f'<c r="{cell_ref}" t="inlineStr"><is><t xml:space="preserve">{escape(str(value))}</t></is></c>'


def worksheet_xml(rows: list[list[Any]]) -> str:
    row_xml: list[str] = []
    max_columns = max((len(row) for row in rows), default=1)
    last_cell = f"{excel_column_name(max_columns)}{max(len(rows), 1)}"
    for row_index, row in enumerate(rows, start=1):
        cells = [xml_cell(f"{excel_column_name(column_index)}{row_index}", value) for column_index, value in enumerate(row, start=1)]
        row_xml.append(f'<row r="{row_index}">{"".join(cells)}</row>')
    return ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">' f'<dimension ref="A1:{last_cell}"/>' '<sheetViews><sheetView workbookViewId="0"/></sheetViews>' '<sheetFormatPr defaultRowHeight="15"/>' f'<sheetData>{"".join(row_xml)}</sheetData>' '</worksheet>')


def build_workbook(sheets: dict[str, list[list[Any]]]) -> bytes:
    buffer = BytesIO()
    workbook_xml = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">' '<sheets>' + ''.join(f'<sheet name="{escape(name)}" sheetId="{index}" r:id="rId{index}"/>' for index, name in enumerate(sheets.keys(), start=1)) + '</sheets></workbook>')
    workbook_rels = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">' + ''.join(f'<Relationship Id="rId{index}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{index}.xml"/>' for index in range(1, len(sheets) + 1)) + '<Relationship Id="rId99" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>' '</Relationships>')
    root_rels = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">' '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>' '</Relationships>')
    content_types = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">' '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>' '<Default Extension="xml" ContentType="application/xml"/>' '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>' '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>' + ''.join(f'<Override PartName="/xl/worksheets/sheet{index}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>' for index in range(1, len(sheets) + 1)) + '</Types>')
    styles_xml = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">' '<fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>' '<fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>' '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>' '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>' '<cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>' '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>' '</styleSheet>')
    with ZipFile(buffer, 'w', ZIP_DEFLATED) as archive:
        archive.writestr('[Content_Types].xml', content_types)
        archive.writestr('_rels/.rels', root_rels)
        archive.writestr('xl/workbook.xml', workbook_xml)
        archive.writestr('xl/_rels/workbook.xml.rels', workbook_rels)
        archive.writestr('xl/styles.xml', styles_xml)
        for index, rows in enumerate(sheets.values(), start=1):
            archive.writestr(f'xl/worksheets/sheet{index}.xml', worksheet_xml(rows))
    return buffer.getvalue()


def normalize_text(value: Any) -> str:
    return re.sub(r'\s+', ' ', str(value or '')).strip()


def parse_float(value: str) -> float | None:
    text = value.replace(',', '').strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def is_meaningful_quantity(value: float | None) -> bool:
    return value is not None and 0 < value < 100000


def load_workbook_rows(path: Path) -> tuple[list[dict[str, Any]], str | None]:
    with ZipFile(path) as workbook:
        workbook_xml = ET.fromstring(workbook.read('xl/workbook.xml'))
        rels_xml = ET.fromstring(workbook.read('xl/_rels/workbook.xml.rels'))
        rel_map = {item.attrib['Id']: item.attrib['Target'] for item in rels_xml}
        shared_strings: list[str] = []
        if 'xl/sharedStrings.xml' in workbook.namelist():
            shared_xml = ET.fromstring(workbook.read('xl/sharedStrings.xml'))
            for si in shared_xml.findall('a:si', XML_NS):
                shared_strings.append(''.join(node.text or '' for node in si.iterfind('.//a:t', XML_NS)))

        def cell_value(cell: ET.Element) -> str:
            cell_type = cell.attrib.get('t')
            inline = cell.find('a:is', XML_NS)
            raw = cell.find('a:v', XML_NS)
            if inline is not None:
                return ''.join(node.text or '' for node in inline.iterfind('.//a:t', XML_NS))
            if raw is None:
                return ''
            value = raw.text or ''
            if cell_type == 's' and value.isdigit():
                index = int(value)
                return shared_strings[index] if index < len(shared_strings) else value
            return value

        sheets = workbook_xml.find('a:sheets', XML_NS)
        if sheets is None:
            return [], None
        parsed_rows: list[dict[str, Any]] = []
        detected_sheet: str | None = None
        for sheet in sheets:
            detected_sheet = sheet.attrib.get('name')
            rel_id = sheet.attrib['{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id']
            target = rel_map[rel_id].lstrip('/')
            if not target.startswith('xl/'):
                target = str(PurePosixPath('xl') / PurePosixPath(target)).lstrip('/')
            sheet_xml = ET.fromstring(workbook.read(target))
            for row in sheet_xml.findall('.//a:sheetData/a:row', XML_NS):
                values = [normalize_text(cell_value(cell)) for cell in row.findall('a:c', XML_NS)]
                values = [value for value in values if value]
                if not values:
                    continue
                description = max((value for value in values if re.search(r'[A-Za-z]', value)), key=len, default='')
                numeric_candidates = [parse_float(value) for value in values]
                quantity = next((value for value in numeric_candidates if is_meaningful_quantity(value)), None)
                parsed_rows.append({'cells': values, 'description': description, 'quantity': quantity})
            if parsed_rows:
                break
        return parsed_rows, detected_sheet


def choose_agent_for_row(description: str) -> dict[str, Any] | None:
    lowered = description.lower()
    ranked: list[tuple[int, int, dict[str, Any]]] = []
    for agent in SPECIALIST_AGENTS:
        score = sum(1 for keyword in agent['keywords'] if keyword in lowered)
        if score:
            ranked.append((score, -agent['sequence'], agent))
    if not ranked:
        return None
    ranked.sort(reverse=True, key=lambda item: (item[0], item[1]))
    return ranked[0][2]


def clean_scope_name(description: str, wbs_category: str) -> str:
    text = normalize_text(description)
    text = re.sub(r'^(item|description|work|scope|boq)\s*[:-]\s*', '', text, flags=re.I)
    text = re.sub(r'\b(quantity|qty|unit|rate|amount)\b.*$', '', text, flags=re.I)
    text = text.strip(' -:;/,')
    return text[:90] or wbs_category


def expand_scope_to_activities(wbs_category: str, scope_name: str) -> list[str]:
    name = scope_name.lower()
    if wbs_category == 'Doors and Partitions':
        return [f'{scope_name} - Material Submittal', f'{scope_name} - Frame Installation', f'{scope_name} - Leaf and Ironmongery Installation'] if 'door' in name else [f'{scope_name} - Setting Out', f'{scope_name} - Framing', f'{scope_name} - Boarding and Finishing']
    if wbs_category == 'Wood Works':
        return [f'{scope_name} - Measurements', f'{scope_name} - Fabrication', f'{scope_name} - Installation']
    if wbs_category == 'Ceiling':
        return [f'{scope_name} - Suspension Grid', f'{scope_name} - Board Fixing', f'{scope_name} - Finishing']
    if wbs_category == 'Floor Finishes':
        return [f'{scope_name} - Layout', f'{scope_name} - Installation', f'{scope_name} - Grouting and Protection'] if any(keyword in name for keyword in ['tile', 'marble', 'granite', 'stone']) else [f'{scope_name} - Surface Preparation', f'{scope_name} - Installation', f'{scope_name} - Final Touches']
    if wbs_category == 'Wall Finishes':
        return [f'{scope_name} - Surface Preparation', f'{scope_name} - First Coat', f'{scope_name} - Final Coat'] if any(keyword in name for keyword in ['paint', 'painting']) else [f'{scope_name} - Base Preparation', f'{scope_name} - Finish Application', f'{scope_name} - Snag Rectification']
    if wbs_category == 'HVAC':
        return [f'{scope_name} - Fabrication', f'{scope_name} - First Fix', f'{scope_name} - Testing and Balancing']
    if wbs_category == 'Electrical':
        return [f'{scope_name} - First Fix', f'{scope_name} - Wiring and Termination', f'{scope_name} - Final Fix']
    if wbs_category == 'Miscellaneous':
        return [f'{scope_name} - Procurement', f'{scope_name} - Installation', f'{scope_name} - Testing and Handover']
    if wbs_category == 'Outdoor Areas':
        return [f'{scope_name} - Coordination', f'{scope_name} - Execution', f'{scope_name} - Inspection and Signoff']
    return [scope_name]

def build_agent_output(agent: dict[str, Any], rows: list[dict[str, Any]]) -> tuple[list[dict[str, str]], int]:
    matches: list[dict[str, Any]] = []
    for row in rows:
        assigned_agent = choose_agent_for_row(row['description'])
        if assigned_agent and assigned_agent['id'] == agent['id']:
            matches.append(row)
    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in matches[:10]:
        scope_name = clean_scope_name(row['description'], agent['wbs_category'])
        for activity_name in expand_scope_to_activities(agent['wbs_category'], scope_name):
            key = (agent['wbs_category'], activity_name)
            if key in seen:
                continue
            seen.add(key)
            deduped.append({'WBS': agent['wbs_category'], 'Activity Name': activity_name})
            if len(deduped) >= 6:
                break
        if len(deduped) >= 6:
            break
    if not deduped:
        deduped = deepcopy(agent['template_output'])
    return deduped, len(matches)


def apply_delay_events(base_date: date, events: list[dict[str, Any]]) -> date:
    adjusted = base_date
    for event in sorted(events, key=lambda item: item['date']):
        event_date = datetime.strptime(event['date'], '%Y-%m-%d').date()
        if event_date <= adjusted:
            adjusted += timedelta(days=event['lost_days'])
    return adjusted


def build_schedule(agents: list[dict[str, Any]], events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    mobilization_start = apply_delay_events(TODAY, events)
    mobilization_finish = mobilization_start + timedelta(days=4)
    schedule: list[dict[str, Any]] = [{'wbs': 'Preliminaries', 'activity_name': 'Site Mobilization', 'duration_days': 5, 'predecessors': 'Project Start', 'resource_list': 'Site Team', 'start_date': mobilization_start.isoformat(), 'finish_date': mobilization_finish.isoformat(), 'package_sequence': 0}]
    closeout_predecessors: list[str] = []
    latest_finish = mobilization_finish
    for agent in sorted(agents, key=lambda item: item['sequence']):
        durations = DURATION_RULES.get(agent['wbs_category'], [2] * max(len(agent['latest_output']), 1))
        package_start = apply_delay_events(mobilization_finish + timedelta(days=1), events)
        previous_activity = 'Site Mobilization'
        package_finish = mobilization_finish
        for index, output in enumerate(agent['latest_output']):
            duration = durations[index] if index < len(durations) else durations[-1]
            start_date = package_start if index == 0 else package_finish + timedelta(days=1)
            start_date = apply_delay_events(start_date, events)
            finish_date = start_date + timedelta(days=duration - 1)
            schedule.append({'wbs': output['WBS'], 'activity_name': output['Activity Name'], 'duration_days': duration, 'predecessors': previous_activity, 'resource_list': agent['resource_list'], 'start_date': start_date.isoformat(), 'finish_date': finish_date.isoformat(), 'package_sequence': agent['sequence']})
            previous_activity = output['Activity Name']
            package_finish = finish_date
        closeout_predecessors.append(previous_activity)
        latest_finish = max(latest_finish, package_finish)
    closeout_start = apply_delay_events(latest_finish + timedelta(days=1), events)
    closeout_finish = closeout_start + timedelta(days=4)
    schedule.append({'wbs': 'Closeout / Testing & Commissioning', 'activity_name': 'Testing, Snagging, and Handover', 'duration_days': 5, 'predecessors': ', '.join(closeout_predecessors), 'resource_list': 'Project Team', 'start_date': closeout_start.isoformat(), 'finish_date': closeout_finish.isoformat(), 'package_sequence': 99})
    schedule.sort(key=lambda item: (item['start_date'], item['package_sequence'], item['activity_name']))
    return schedule


def excel_serial(date_string: str) -> int:
    return (datetime.strptime(date_string, '%Y-%m-%d').date() - date(1899, 12, 30)).days


def task_code(index: int) -> str:
    return f'BOQ{index * 10:04d}'


def build_wbs_code(wbs_name: str) -> str:
    return PACKAGE_WBS_CODES.get(wbs_name, f"{PRIMAVERA_PROJECT_ID}.{re.sub(r'[^A-Za-z0-9]+', '', wbs_name)[:20]}")


def build_primavera_rows(schedule: list[dict[str, Any]]) -> dict[str, list[list[Any]]]:
    activity_ids = {item['activity_name']: task_code(index) for index, item in enumerate(schedule, start=1)}
    task_rows: list[list[Any]] = [['task_code', 'status_code', 'wbs_id', 'task_name', 'start_date', 'end_date', 'resource_list', 'delete_record_flag'], ['Activity ID', 'Activity Status', 'WBS Code', 'Activity Name', '(*)Start', '(*)Finish', '(*)Resources', 'Delete This Row']]
    relationship_rows: list[list[Any]] = [['pred_task_id', 'task_id', 'pred_type', 'PREDTASK__status_code', 'TASK__status_code', 'pred_proj_id', 'proj_id', 'PREDTASK__PROJWBS__wbs_full_name', 'TASK__PROJWBS__wbs_full_name', 'PREDTASK__task_name', 'TASK__task_name', 'lag_hr_cnt', 'PREDTASK__rsrc_id', 'TASK__rsrc_id', 'delete_record_flag'], ['Predecessor', 'Successor', 'Relationship Type', '(*)Predecessor Activity Status', '(*)Successor Activity Status', '(*)Predecessor Project', '(*)Successor Project', '(*)Predecessor WBS', '(*)Successor WBS', '(*)Predecessor Activity Name', '(*)Successor Activity Name', 'Lag(h)', '(*)Predecessor Primary Resource', '(*)Successor Primary Resource', 'Delete This Row']]
    activity_lookup = {item['activity_name']: item for item in schedule}
    for item in schedule:
        task_rows.append([activity_ids[item['activity_name']], 'Not Started', build_wbs_code(item['wbs']), item['activity_name'], excel_serial(item['start_date']), excel_serial(item['finish_date']), item.get('resource_list', ''), ''])
        predecessors = [part.strip() for part in item['predecessors'].split(',') if part.strip() and part.strip() != 'Project Start']
        for predecessor_name in predecessors:
            predecessor_item = activity_lookup.get(predecessor_name)
            if not predecessor_item:
                continue
            relationship_rows.append([activity_ids[predecessor_name], activity_ids[item['activity_name']], 'FS', 'Not Started', 'Not Started', PRIMAVERA_PROJECT_ID, PRIMAVERA_PROJECT_ID, f"{build_wbs_code(predecessor_item['wbs'])} {predecessor_item['wbs']}", f"{build_wbs_code(item['wbs'])} {item['wbs']}", predecessor_name, item['activity_name'], 0, predecessor_item.get('resource_list', ''), item.get('resource_list', ''), ''])
    userdata_rows = [['user_data'], ['UserSettings Do Not Edit'], ['DurationQtyType=QT_Hour\nShowAsPercentage=0\nSmallScaleQtyType=QT_Hour\nDateFormat=m/d/yyyy\nCurrencyFormat=US Dollar\n']]
    return {'TASK': task_rows, 'TASKPRED': relationship_rows, 'USERDATA': userdata_rows}


def build_primavera_workbook(schedule: list[dict[str, Any]]) -> bytes:
    return build_workbook(build_primavera_rows(schedule))


def refresh_primavera_export(state: dict[str, Any]) -> None:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    PRIMAVERA_EXPORT_FILE.write_bytes(build_primavera_workbook(state['timeline']['schedule']))
    state['planner']['export_file'] = PRIMAVERA_EXPORT_FILE.name
    state['planner']['export_updated_at'] = datetime.now().isoformat(timespec='seconds')


def recalculate_timeline(state: dict[str, Any], action: str) -> None:
    schedule = build_schedule(state['agents'], state['timeline']['events'])
    schedule.sort(key=lambda item: (item['start_date'], item['package_sequence'], item['activity_name']))
    state['timeline']['schedule'] = schedule
    state['timeline']['start_date'] = min(item['start_date'] for item in schedule)
    state['timeline']['finish_date'] = max(item['finish_date'] for item in schedule)
    state['project_summary'] = {'total_duration_days': sum(item['duration_days'] for item in schedule), 'delay_events': len(state['timeline']['events']), 'last_action': action, 'primavera_rows': len(schedule)}


async def run_specialist_agent(agent: dict[str, Any], boq_rows: list[dict[str, Any]]) -> None:
    agent['status'] = 'running'
    await asyncio.sleep(0)
    latest_output, matches = build_agent_output(agent, boq_rows)
    agent['latest_output'] = latest_output
    agent['boq_matches'] = matches
    agent['last_run'] = datetime.now().isoformat(timespec='seconds')
    agent['status'] = 'completed'


async def run_full_workflow_logic(state: dict[str, Any]) -> str:
    stored_path = state['boq_upload'].get('stored_path')
    if not stored_path:
        raise HTTPException(status_code=400, detail='Upload a BOQ workbook before running the workflow.')
    path = Path(stored_path)
    if not path.exists():
        raise HTTPException(status_code=404, detail='The uploaded BOQ file could not be found on disk.')
    try:
        boq_rows, detected_sheet = load_workbook_rows(path)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f'Failed to parse the uploaded BOQ workbook: {exc}') from exc
    state['workflow']['status'] = 'running'
    state['planner']['status'] = 'waiting'
    for agent in state['agents']:
        agent['status'] = 'queued'
    await asyncio.gather(*(run_specialist_agent(agent, boq_rows) for agent in state['agents']))
    state['boq_upload']['row_count'] = len(boq_rows)
    state['boq_upload']['detected_sheet'] = detected_sheet
    state['boq_upload']['status'] = 'BOQ parsed. All specialist agents completed, and the project manager prepared the Primavera import workbook.'
    state['planner']['status'] = 'exported'
    state['planner']['last_run'] = datetime.now().isoformat(timespec='seconds')
    state['workflow']['status'] = 'completed'
    state['workflow']['last_run'] = datetime.now().isoformat(timespec='seconds')
    recalculate_timeline(state, 'Ran full BOQ workflow with parallel specialist agents')
    refresh_primavera_export(state)
    total_activities = sum(len(agent['latest_output']) for agent in state['agents'])
    return f"Ran {len(state['agents'])} specialist agents in parallel from the uploaded BOQ, generated {total_activities} package activities, and refreshed {state['planner']['export_file']} for Primavera import."

def run_agent_logic(state: dict[str, Any], agent_id: str) -> str:
    for agent in state['agents']:
        if agent['id'] == agent_id:
            latest_output, matches = build_agent_output(agent, [])
            agent['latest_output'] = latest_output
            agent['boq_matches'] = matches
            agent['last_run'] = datetime.now().isoformat(timespec='seconds')
            agent['status'] = 'completed'
            recalculate_timeline(state, f"Ran {agent['wbs_category']} agent")
            return f"{agent['wbs_category']} agent refreshed {len(agent['latest_output'])} activities."
    if agent_id == PLANNER_AGENT['id']:
        recalculate_timeline(state, 'Project Manager Agent rebuilt the Primavera export')
        state['planner']['status'] = 'exported'
        state['planner']['last_run'] = datetime.now().isoformat(timespec='seconds')
        refresh_primavera_export(state)
        return f"{state['planner']['name']} rebuilt the Primavera workbook."
    return 'Agent not found.'


def find_agent(state: dict[str, Any], message: str) -> dict[str, Any] | None:
    lowered = message.lower()
    for agent in state['agents']:
        if agent['wbs_category'].lower() in lowered or agent['agent_name'].lower() in lowered:
            return agent
    if 'planner' in lowered or 'schedule' in lowered or 'workflow' in lowered:
        return {'id': PLANNER_AGENT['id']}
    return None


def parse_lost_days(message: str) -> int:
    match = re.search(r'(\d+)\s*(day|days)', message.lower())
    return max(1, int(match.group(1))) if match else 1


def add_delay_event(state: dict[str, Any], reason: str, lost_days: int, event_date: str | None) -> str:
    parsed_date = event_date or TODAY.isoformat()
    state['timeline']['events'].append({'id': f"event-{len(state['timeline']['events']) + 1}", 'date': parsed_date, 'reason': reason, 'lost_days': lost_days})
    recalculate_timeline(state, f'Logged delay event on {parsed_date}')
    return f"I logged {lost_days} lost day(s) on {parsed_date} and recalculated the finish date to {state['timeline']['finish_date']}."


def summarize_timeline(state: dict[str, Any]) -> str:
    return f"The current plan has {len(state['timeline']['schedule'])} schedule rows, {state['project_summary']['delay_events']} delay event(s), and a finish date of {state['timeline']['finish_date']}."


def explain_agent(agent: dict[str, Any]) -> str:
    if agent.get('id') == PLANNER_AGENT['id']:
        return f"{PLANNER_AGENT['name']} waits for all specialist agents to finish, then consolidates their outputs into the concurrent package schedule and formats the Primavera import workbook."
    return f"{agent['wbs_category']} is handled by {agent['agent_name']}. It reads the uploaded BOQ rows assigned to that package and turns them into sequenced scheduler-ready activities. Latest output count: {len(agent['latest_output'])}."


def handle_chat(state: dict[str, Any], message: str) -> str:
    lowered = message.lower()
    agent = find_agent(state, message)
    if any(phrase in lowered for phrase in ["couldn't work", 'could not work', 'missed today', 'delay', 'lost day', 'couldnt work']):
        return add_delay_event(state, message.strip(), parse_lost_days(message), TODAY.isoformat())
    if 'run workflow' in lowered or 'run all' in lowered:
        return 'Use the Run Workflow button to launch all specialist agents together and refresh the Primavera workbook.'
    if 'run' in lowered and agent:
        return run_agent_logic(state, agent['id'])
    if any(keyword in lowered for keyword in ['explain', 'what does', 'who is', 'agent']) and agent:
        return explain_agent(agent)
    if any(keyword in lowered for keyword in ['timeline', 'schedule', 'finish', 'completion', 'recalculate']):
        recalculate_timeline(state, 'Schedule reviewed in chat')
        return summarize_timeline(state)
    return 'I can explain a package agent, describe the workflow, or log a delay event and rebuild the schedule.'


app = FastAPI(title='BOQ Agent Console API')
app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_methods=['*'], allow_headers=['*'])
STATE = load_state()


@app.api_route('/health', methods=['GET', 'HEAD'])
def health_check() -> dict[str, str]:
    return {'status': 'ok', 'service': 'boq-agent-console-api', 'timestamp': datetime.now().isoformat(timespec='seconds')}


@app.get('/api/dashboard')
def get_dashboard() -> dict[str, Any]:
    return STATE


@app.post('/api/agents/{agent_id}/run')
def run_agent(agent_id: str) -> dict[str, Any]:
    result = run_agent_logic(STATE, agent_id)
    append_chat(STATE, 'assistant', result)
    save_state(STATE)
    return STATE


@app.post('/api/workflow/run')
async def run_workflow() -> dict[str, Any]:
    result = await run_full_workflow_logic(STATE)
    append_chat(STATE, 'assistant', result)
    save_state(STATE)
    return STATE


@app.post('/api/boq/upload')
async def upload_boq(request: Request) -> dict[str, Any]:
    filename = request.headers.get('x-filename', 'uploaded_boq.xlsx')
    extension = Path(filename).suffix.lower()
    if extension not in {'.xlsx', '.xls'}:
        raise HTTPException(status_code=400, detail='Only Excel BOQ files (.xlsx or .xls) are supported.')
    file_bytes = await request.body()
    if not file_bytes:
        raise HTTPException(status_code=400, detail='Uploaded BOQ file is empty.')
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    safe_name = re.sub(r'[^A-Za-z0-9._-]+', '_', Path(filename).name or 'uploaded_boq.xlsx')
    stored_path = UPLOAD_DIR / f'{timestamp}-{safe_name}'
    stored_path.write_bytes(file_bytes)
    for agent in STATE['agents']:
        agent['status'] = 'ready'
        agent['latest_output'] = deepcopy(agent['template_output'])
        agent['boq_matches'] = 0
    STATE['planner']['status'] = 'ready'
    STATE['workflow']['status'] = 'ready'
    STATE['boq_upload'] = {'filename': filename, 'stored_path': str(stored_path), 'uploaded_at': datetime.now().isoformat(timespec='seconds'), 'status': 'BOQ uploaded and ready for the full workflow run.', 'row_count': 0, 'detected_sheet': None}
    STATE['project_summary']['last_action'] = f'Uploaded BOQ file {filename}'
    append_chat(STATE, 'assistant', f"BOQ file '{filename}' uploaded successfully. Press Run Workflow to launch all specialist agents together.")
    save_state(STATE)
    return STATE


@app.get('/api/timeline')
def get_timeline() -> dict[str, Any]:
    return STATE['timeline']


@app.get('/api/exports/primavera.xlsx')
def download_primavera_export() -> FileResponse:
    refresh_primavera_export(STATE)
    return FileResponse(PRIMAVERA_EXPORT_FILE, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', filename='primavera_schedule_import.xlsx')


@app.post('/api/timeline/events')
def add_timeline_event(request: TimelineEventRequest) -> dict[str, Any]:
    result = add_delay_event(STATE, request.reason, max(1, request.lost_days), request.date)
    append_chat(STATE, 'assistant', result)
    save_state(STATE)
    return STATE


@app.post('/api/chat')
def chat(request: ChatRequest) -> dict[str, Any]:
    append_chat(STATE, 'user', request.message)
    response = handle_chat(STATE, request.message)
    append_chat(STATE, 'assistant', response)
    save_state(STATE)
    return STATE


if __name__ == '__main__':
    uvicorn.run('backend.main:app', host=os.getenv('HOST', '0.0.0.0'), port=int(os.getenv('PORT', '8000')))
