from __future__ import annotations

import json
from io import BytesIO
import re
from copy import deepcopy
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape
from zipfile import ZIP_DEFLATED, ZipFile

from fastapi import FastAPI
from fastapi import HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel


APP_DIR = Path(__file__).resolve().parent
STATE_FILE = APP_DIR / "project_state.json"
EXPORT_DIR = APP_DIR / "exports"
UPLOAD_DIR = APP_DIR / "uploads"
PRIMAVERA_EXPORT_FILE = EXPORT_DIR / "primavera_schedule_import.xlsx"
TODAY = date(2026, 3, 11)
PRIMAVERA_HOURS_PER_DAY = 8

SPECIALIST_AGENTS = [
    {
        "id": "agent-1",
        "agent_name": "WBS_Extractor_01",
        "wbs_category": "Doors and Partitions",
        "sequence": 1,
        "status": "active",
        "task": "Extract all valid activities from the BOQ under the Doors and Partitions WBS category, skip zero quantity entries, and split compound works into scheduler-ready subtasks.",
        "language_guidelines": {
            "clarity": "Use concise, descriptive construction terminology.",
            "granularity": "Decompose tasks into sequential, detailed subtasks.",
            "formatting": "Skip zero quantity work and vague wording.",
        },
        "template_output": [
            {"WBS": "Doors and Partitions", "Activity Name": "Partitions - Marking"},
            {"WBS": "Doors and Partitions", "Activity Name": "Partitions - Framing"},
            {"WBS": "Doors and Partitions", "Activity Name": "Doors - Frame Installation"},
            {"WBS": "Doors and Partitions", "Activity Name": "Doors - Shutter Hanging"},
        ],
    },
    {
        "id": "agent-2",
        "agent_name": "WBS_Extractor_02",
        "wbs_category": "Wood Works",
        "sequence": 2,
        "status": "active",
        "task": "Extract Wood Works activities, ignore zero quantity items, and split joinery into measurement, fabrication, finishing, and installation steps.",
        "language_guidelines": {
            "clarity": "Use full descriptive task names.",
            "granularity": "Subdivide fabrication, finishing, and installation steps.",
            "formatting": "Only include measurable, real tasks with quantity.",
        },
        "template_output": [
            {"WBS": "Wood Works", "Activity Name": "Joinery - Site Measurements"},
            {"WBS": "Wood Works", "Activity Name": "Joinery - Fabrication"},
            {"WBS": "Wood Works", "Activity Name": "Joinery - Finishing"},
            {"WBS": "Wood Works", "Activity Name": "Joinery - Installation"},
        ],
    },
    {
        "id": "agent-3",
        "agent_name": "WBS_Extractor_03",
        "wbs_category": "Ceiling",
        "sequence": 3,
        "status": "active",
        "task": "Extract Ceiling activities, ignore zero quantity lines, and separate framing, board fixing, putty, and paint coats into clean JSON entries.",
        "language_guidelines": {
            "clarity": "Include action and material in activity names.",
            "granularity": "Split multi-step work into sequential subtasks.",
            "formatting": "Skip non-applicable entries.",
        },
        "template_output": [
            {"WBS": "Ceiling", "Activity Name": "Ceiling - Framing"},
            {"WBS": "Ceiling", "Activity Name": "Ceiling - Board Fixing"},
            {"WBS": "Ceiling", "Activity Name": "Ceiling - Putty"},
            {"WBS": "Ceiling", "Activity Name": "Ceiling - Paint Final Coat"},
        ],
    },
    {
        "id": "agent-4",
        "agent_name": "WBS_Extractor_04",
        "wbs_category": "Floor Finishes",
        "sequence": 4,
        "status": "active",
        "task": "Identify floor finish activities, exclude zero quantity lines, and split tiling and finish systems into layout, installation, and closeout steps.",
        "language_guidelines": {
            "clarity": "Avoid shorthand in activity names.",
            "granularity": "List each construction phase.",
            "formatting": "Avoid duplicate or null entries.",
        },
        "template_output": [
            {"WBS": "Floor Finishes", "Activity Name": "Floor Finishes - Layout"},
            {"WBS": "Floor Finishes", "Activity Name": "Ceramic Tiles - Setting"},
            {"WBS": "Floor Finishes", "Activity Name": "Ceramic Tiles - Grouting"},
            {"WBS": "Floor Finishes", "Activity Name": "Raised Floor - Installation"},
        ],
    },
    {
        "id": "agent-5",
        "agent_name": "WBS_Extractor_05",
        "wbs_category": "Wall Finishes",
        "sequence": 5,
        "status": "active",
        "task": "Extract Wall Finishes tasks, exclude zero quantity items, and break painting into putty and coat-by-coat scheduling activities.",
        "language_guidelines": {
            "clarity": "Use clear scheduler-ready wording.",
            "granularity": "Split each painting or finishing layer.",
            "formatting": "Filter non-workable entries.",
        },
        "template_output": [
            {"WBS": "Wall Finishes", "Activity Name": "Wall Finishes - Putty"},
            {"WBS": "Wall Finishes", "Activity Name": "Paint - First Coat"},
            {"WBS": "Wall Finishes", "Activity Name": "Paint - Second Coat"},
            {"WBS": "Wall Finishes", "Activity Name": "Paint - Final Coat"},
        ],
    },
    {
        "id": "agent-6",
        "agent_name": "WBS_Extractor_06",
        "wbs_category": "HVAC",
        "sequence": 6,
        "status": "active",
        "task": "Capture HVAC tasks, skip zero quantity lines, and split systems like ducting into fabrication, first fix, second fix, insulation, and finals.",
        "language_guidelines": {
            "clarity": "Use MEP-compliant language.",
            "granularity": "Break down work by component and stage.",
            "formatting": "Do not include zero or undefined work.",
        },
        "template_output": [
            {"WBS": "HVAC", "Activity Name": "Ducting - Fabrication"},
            {"WBS": "HVAC", "Activity Name": "Ducting - First Fix"},
            {"WBS": "HVAC", "Activity Name": "Ducting - Insulation"},
            {"WBS": "HVAC", "Activity Name": "FCU - Final Installation"},
        ],
    },
    {
        "id": "agent-7",
        "agent_name": "WBS_Extractor_07",
        "wbs_category": "Electrical",
        "sequence": 7,
        "status": "active",
        "task": "Extract electrical tasks for lighting, power, low current, and fire alarm, ignore zero quantity lines, and split them into conduit, wiring, and fixture installation phases.",
        "language_guidelines": {
            "clarity": "Use IEC or electrical standard terms.",
            "granularity": "Separate low-current and high-power systems.",
            "formatting": "Format for P6 or Excel use.",
        },
        "template_output": [
            {"WBS": "Electrical", "Activity Name": "Lighting - Conduit First Fix"},
            {"WBS": "Electrical", "Activity Name": "Lighting - Wiring First Fix"},
            {"WBS": "Electrical", "Activity Name": "Power - Socket Installation"},
            {"WBS": "Electrical", "Activity Name": "Fire Alarm - Control Panel Installation"},
        ],
    },
    {
        "id": "agent-8",
        "agent_name": "WBS_Extractor_08",
        "wbs_category": "Miscellaneous",
        "sequence": 8,
        "status": "active",
        "task": "Extract miscellaneous work such as waterproofing, signage, and temporary works, skip zero quantity lines, and rewrite vague items into scope-based tasks.",
        "language_guidelines": {
            "clarity": "Avoid placeholders or generic text.",
            "granularity": "Convert umbrella items into real tasks.",
            "formatting": "Keep one task per line.",
        },
        "template_output": [
            {"WBS": "Miscellaneous", "Activity Name": "Waterproofing - Membrane Installation"},
            {"WBS": "Miscellaneous", "Activity Name": "Signage - Installation"},
            {"WBS": "Miscellaneous", "Activity Name": "Testing - Plumbing Leak Check"},
            {"WBS": "Miscellaneous", "Activity Name": "Branding - Accessories Fixing"},
        ],
    },
    {
        "id": "agent-9",
        "agent_name": "WBS_Extractor_09",
        "wbs_category": "Outdoor Areas",
        "sequence": 9,
        "status": "active",
        "task": "Capture external and landlord-driven works, skip zero quantity lines, and turn vague approval items into schedulable coordination activities.",
        "language_guidelines": {
            "clarity": "Describe approval steps and access tasks clearly.",
            "granularity": "Show real tasks even for coordination items.",
            "formatting": "Avoid placeholders or notes.",
        },
        "template_output": [
            {"WBS": "Outdoor Areas", "Activity Name": "Landlord Approval - Shop Drawings"},
            {"WBS": "Outdoor Areas", "Activity Name": "External Paving - Installation"},
            {"WBS": "Outdoor Areas", "Activity Name": "Access Coordination - Permit Clearance"},
            {"WBS": "Outdoor Areas", "Activity Name": "Landlord Signoff - Final Walkthrough"},
        ],
    },
]

PLANNER_AGENT = {
    "id": "agent-10",
    "name": "Project Manager Agent",
    "role": "PMP construction planner",
    "goal": "Create P6-ready schedules from BOQ, WBS, and scope inputs without follow-up questions.",
    "flow": [
        "Read uploaded BOQ",
        "Extract tasks",
        "Map to WBS",
        "Estimate durations from benchmarks",
        "Sequence with natural predecessors",
        "Output Primavera-ready Excel",
    ],
}

DURATION_RULES = {
    "Doors and Partitions": [2, 4, 3, 2],
    "Wood Works": [2, 4, 3, 3],
    "Ceiling": [3, 3, 2, 2],
    "Floor Finishes": [2, 4, 2, 3],
    "Wall Finishes": [2, 2, 2, 2],
    "HVAC": [4, 5, 3, 3],
    "Electrical": [3, 4, 3, 2],
    "Miscellaneous": [2, 2, 2, 2],
    "Outdoor Areas": [2, 4, 2, 2],
}


class ChatRequest(BaseModel):
    message: str


class TimelineEventRequest(BaseModel):
    date: str | None = None
    reason: str
    lost_days: int = 1


def seed_state() -> dict[str, Any]:
    agents = []
    for agent in SPECIALIST_AGENTS:
        copy = deepcopy(agent)
        copy["latest_output"] = deepcopy(agent["template_output"])
        copy["last_run"] = None
        agents.append(copy)

    schedule = build_schedule(agents, [])
    return {
        "agents": agents,
        "planner": {
            **deepcopy(PLANNER_AGENT),
            "status": "ready",
            "last_run": None,
            "export_file": PRIMAVERA_EXPORT_FILE.name,
            "export_updated_at": None,
        },
        "boq_upload": {
            "filename": None,
            "stored_path": None,
            "uploaded_at": None,
            "status": "No BOQ uploaded yet",
        },
        "timeline": {
            "start_date": TODAY.isoformat(),
            "finish_date": schedule[-1]["finish_date"],
            "schedule": schedule,
            "events": [],
        },
        "chat_history": [
            {
                "role": "assistant",
                "content": "I can explain any BOQ agent, run one for you, or log a lost workday and recalculate the full schedule.",
            }
        ],
        "project_summary": {
            "total_duration_days": sum(item["duration_days"] for item in schedule),
            "delay_events": 0,
            "last_action": "Dashboard initialized",
        },
    }


def load_state() -> dict[str, Any]:
    if STATE_FILE.exists():
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        state.setdefault(
            "planner",
            {
                **deepcopy(PLANNER_AGENT),
                "status": "ready",
                "last_run": None,
                "export_file": PRIMAVERA_EXPORT_FILE.name,
                "export_updated_at": None,
            },
        )
        state["planner"].setdefault("status", "ready")
        state["planner"].setdefault("last_run", None)
        state["planner"]["name"] = PLANNER_AGENT["name"]
        state["planner"]["role"] = PLANNER_AGENT["role"]
        state["planner"]["goal"] = PLANNER_AGENT["goal"]
        state["planner"]["flow"] = PLANNER_AGENT["flow"]
        state["planner"]["export_file"] = PRIMAVERA_EXPORT_FILE.name
        state["planner"].setdefault("export_updated_at", None)
        state.setdefault(
            "boq_upload",
            {
                "filename": None,
                "stored_path": None,
                "uploaded_at": None,
                "status": "No BOQ uploaded yet",
            },
        )
        refresh_primavera_export(state)
        save_state(state)
        return state
    state = seed_state()
    save_state(state)
    return state


def save_state(state: dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
    refresh_primavera_export(state)


def build_wbs_codes(schedule: list[dict[str, Any]]) -> dict[str, str]:
    preferred_codes = {
        "Preliminaries": "PRELIM",
        "Doors and Partitions": "INT-DOORS",
        "Wood Works": "INT-WOOD",
        "Ceiling": "INT-CEIL",
        "Floor Finishes": "INT-FLOOR",
        "Wall Finishes": "INT-WALL",
        "HVAC": "MEP-HVAC",
        "Electrical": "MEP-ELEC",
        "Miscellaneous": "MISC",
        "Outdoor Areas": "EXT-AREAS",
        "Closeout / Testing & Commissioning": "CLOSEOUT",
    }
    codes: dict[str, str] = {}
    used_codes: set[str] = set()
    for item in schedule:
        wbs_name = item["wbs"]
        if wbs_name in codes:
            continue
        candidate = preferred_codes.get(wbs_name)
        if not candidate:
            candidate = re.sub(r"[^A-Z0-9]+", "-", wbs_name.upper()).strip("-")[:30] or "WBS"
        suffix = 1
        base = candidate
        while candidate in used_codes:
            suffix += 1
            candidate = f"{base[:26]}-{suffix}"
        codes[wbs_name] = candidate
        used_codes.add(candidate)
    return codes


def build_primavera_rows(schedule: list[dict[str, Any]]) -> dict[str, list[list[Any]]]:
    wbs_codes = build_wbs_codes(schedule)
    activity_id_by_name: dict[str, str] = {}
    activity_rows: list[list[Any]] = []
    relationship_rows: list[list[Any]] = []
    review_rows: list[list[Any]] = []

    for index, item in enumerate(schedule, start=1):
        activity_id = f"A{index * 10:04d}"
        activity_id_by_name[item["activity_name"]] = activity_id

        constraint_type = "Start On" if index == 1 else ""
        constraint_date = item["start_date"] if index == 1 else ""
        duration_hours = item["duration_days"] * PRIMAVERA_HOURS_PER_DAY

        activity_rows.append(
            [
                activity_id,
                item["activity_name"],
                "Not Started",
                wbs_codes[item["wbs"]],
                duration_hours,
                constraint_type,
                constraint_date,
            ]
        )

        review_rows.append(
            [
                activity_id,
                item["wbs"],
                wbs_codes[item["wbs"]],
                item["activity_name"],
                item["duration_days"],
                duration_hours,
                item["predecessors"],
                item["start_date"],
                item["finish_date"],
            ]
        )

    for item in schedule[1:]:
        predecessor_name = item["predecessors"]
        predecessor_id = activity_id_by_name.get(predecessor_name)
        successor_id = activity_id_by_name[item["activity_name"]]
        if predecessor_id:
            relationship_rows.append(
                [
                    successor_id,
                    predecessor_id,
                    "Finish to Start",
                    0,
                ]
            )

    readme_rows = [
        ["This workbook is generated from the backend schedule and prepared for Primavera XLSX import."],
        ["Import sheet", "Activities"],
        ["Relationship sheet", "Activity Relationships"],
        ["Reference sheet", "Schedule Review"],
        ["Hours per day assumption", PRIMAVERA_HOURS_PER_DAY],
        ["Constraint strategy", "The first activity is constrained with Start On using the project start date; the rest are driven by FS relationships and durations."],
        ["WBS note", "WBS codes in the import sheet must exist in the target Primavera project before import if your environment does not create them automatically."],
    ]

    return {
        "Activities": [
            ["task_code", "task_name", "status_code", "wbs_id", "target_drtn_hr_cnt", "cstr_type", "cstr_date"],
            ["Activity ID", "Activity Name", "Activity Status", "WBS Code", "Original Duration", "Primary Constraint", "Primary Constraint Date"],
            *activity_rows,
        ],
        "Activity Relationships": [
            ["task_id", "pred_task_id", "pred_type", "lag_hr_cnt"],
            ["Successor", "Predecessor", "Relationship Type", "Lag"],
            *relationship_rows,
        ],
        "Schedule Review": [
            ["Activity ID", "WBS", "WBS Code", "Activity Name", "Duration Days", "Duration Hours", "Predecessor", "Start Date", "Finish Date"],
            *review_rows,
        ],
        "Read Me": readme_rows,
    }


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
    text = escape(str(value))
    return f'<c r="{cell_ref}" t="inlineStr"><is><t xml:space="preserve">{text}</t></is></c>'


def worksheet_xml(rows: list[list[Any]]) -> str:
    row_xml: list[str] = []
    max_columns = max((len(row) for row in rows), default=1)
    last_cell = f"{excel_column_name(max_columns)}{max(len(rows), 1)}"

    for row_index, row in enumerate(rows, start=1):
        cells = [
            xml_cell(f"{excel_column_name(column_index)}{row_index}", value)
            for column_index, value in enumerate(row, start=1)
        ]
        row_xml.append(f'<row r="{row_index}">{"".join(cells)}</row>')

    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<dimension ref="A1:{last_cell}"/>'
        '<sheetViews><sheetView workbookViewId="0"/></sheetViews>'
        '<sheetFormatPr defaultRowHeight="15"/>'
        f'<sheetData>{"".join(row_xml)}</sheetData>'
        '</worksheet>'
    )


def build_primavera_workbook(schedule: list[dict[str, Any]]) -> bytes:
    sheets = build_primavera_rows(schedule)
    buffer = BytesIO()

    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets>'
        + "".join(
            f'<sheet name="{escape(name)}" sheetId="{index}" r:id="rId{index}"/>'
            for index, name in enumerate(sheets.keys(), start=1)
        )
        + "</sheets></workbook>"
    )

    workbook_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        + "".join(
            f'<Relationship Id="rId{index}" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
            f'Target="worksheets/sheet{index}.xml"/>'
            for index in range(1, len(sheets) + 1)
        )
        + '<Relationship Id="rId99" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
        'Target="styles.xml"/>'
        '</Relationships>'
    )

    root_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/>'
        '</Relationships>'
    )

    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/styles.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        + "".join(
            f'<Override PartName="/xl/worksheets/sheet{index}.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            for index in range(1, len(sheets) + 1)
        )
        + '</Types>'
    )

    styles_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>'
        '<fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>'
        '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
        '<cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>'
        '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
        '</styleSheet>'
    )

    with ZipFile(buffer, "w", ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("_rels/.rels", root_rels)
        archive.writestr("xl/workbook.xml", workbook_xml)
        archive.writestr("xl/_rels/workbook.xml.rels", workbook_rels)
        archive.writestr("xl/styles.xml", styles_xml)
        for index, rows in enumerate(sheets.values(), start=1):
            archive.writestr(f"xl/worksheets/sheet{index}.xml", worksheet_xml(rows))

    return buffer.getvalue()


def refresh_primavera_export(state: dict[str, Any]) -> None:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    workbook = build_primavera_workbook(state["timeline"]["schedule"])
    PRIMAVERA_EXPORT_FILE.write_bytes(workbook)
    planner = state.get("planner")
    if isinstance(planner, dict):
        planner["export_file"] = PRIMAVERA_EXPORT_FILE.name
        planner["export_updated_at"] = datetime.now().isoformat(timespec="seconds")


def build_schedule(agents: list[dict[str, Any]], events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    schedule = [
        {
            "wbs": "Preliminaries",
            "activity_name": "Site Mobilization",
            "duration_days": 5,
            "predecessors": "Project Start",
            "start_date": TODAY.isoformat(),
            "finish_date": (TODAY + timedelta(days=4)).isoformat(),
        }
    ]

    day_cursor = TODAY + timedelta(days=5)
    for event in sorted(events, key=lambda item: item["date"]):
        event_date = datetime.strptime(event["date"], "%Y-%m-%d").date()
        if event_date <= day_cursor:
            day_cursor += timedelta(days=event["lost_days"])

    previous_activity = "Site Mobilization"
    for agent in sorted(agents, key=lambda item: item["sequence"]):
        durations = DURATION_RULES.get(agent["wbs_category"], [2] * len(agent["latest_output"]))
        for index, output in enumerate(agent["latest_output"]):
            duration = durations[index] if index < len(durations) else durations[-1]
            start_date = day_cursor
            finish_date = start_date + timedelta(days=duration - 1)
            schedule.append(
                {
                    "wbs": output["WBS"],
                    "activity_name": output["Activity Name"],
                    "duration_days": duration,
                    "predecessors": previous_activity,
                    "start_date": start_date.isoformat(),
                    "finish_date": finish_date.isoformat(),
                }
            )
            previous_activity = output["Activity Name"]
            day_cursor = finish_date + timedelta(days=1)

    closeout_start = day_cursor
    closeout_finish = closeout_start + timedelta(days=4)
    schedule.append(
        {
            "wbs": "Closeout / Testing & Commissioning",
            "activity_name": "Testing, Snagging, and Handover",
            "duration_days": 5,
            "predecessors": previous_activity,
            "start_date": closeout_start.isoformat(),
            "finish_date": closeout_finish.isoformat(),
        }
    )
    return schedule


def recalculate_timeline(state: dict[str, Any], action: str) -> None:
    schedule = build_schedule(state["agents"], state["timeline"]["events"])
    state["timeline"]["schedule"] = schedule
    state["timeline"]["start_date"] = TODAY.isoformat()
    state["timeline"]["finish_date"] = schedule[-1]["finish_date"]
    state["project_summary"] = {
        "total_duration_days": sum(item["duration_days"] for item in schedule),
        "delay_events": len(state["timeline"]["events"]),
        "last_action": action,
    }


def run_agent_logic(state: dict[str, Any], agent_id: str) -> str:
    if agent_id == PLANNER_AGENT["id"]:
        recalculate_timeline(state, "Project Manager Agent rebuilt the Primavera export")
        planner = state["planner"]
        planner["status"] = "exported"
        planner["last_run"] = datetime.now().isoformat(timespec="seconds")
        refresh_primavera_export(state)
        return (
            f"{planner['name']} rebuilt the full schedule and refreshed "
            f"{planner['export_file']} for Primavera import."
        )

    for agent in state["agents"]:
        if agent["id"] == agent_id:
            agent["latest_output"] = deepcopy(agent["template_output"])
            agent["last_run"] = datetime.now().isoformat(timespec="seconds")
            recalculate_timeline(state, f"Ran {agent['wbs_category']} agent")
            return f"{agent['wbs_category']} agent reviewed the BOQ scope and refreshed {len(agent['latest_output'])} scheduler-ready activities."
    return "Agent not found."


def append_chat(state: dict[str, Any], role: str, content: str) -> None:
    state["chat_history"].append({"role": role, "content": content})


def find_agent(state: dict[str, Any], message: str) -> dict[str, Any] | None:
    lowered = message.lower()
    for agent in state["agents"]:
        if agent["wbs_category"].lower() in lowered or agent["agent_name"].lower() in lowered:
            return agent
    if "planner" in lowered or "schedule" in lowered:
        return {"id": PLANNER_AGENT["id"]}
    return None


def parse_lost_days(message: str) -> int:
    match = re.search(r"(\d+)\s*(day|days)", message.lower())
    return max(1, int(match.group(1))) if match else 1


def add_delay_event(state: dict[str, Any], reason: str, lost_days: int, event_date: str | None) -> str:
    parsed_date = event_date or TODAY.isoformat()
    state["timeline"]["events"].append(
        {
            "id": f"event-{len(state['timeline']['events']) + 1}",
            "date": parsed_date,
            "reason": reason,
            "lost_days": lost_days,
        }
    )
    recalculate_timeline(state, f"Logged delay event on {parsed_date}")
    return f"I logged {lost_days} lost day(s) on {parsed_date} for '{reason}' and recalculated the project finish to {state['timeline']['finish_date']}."


def summarize_timeline(state: dict[str, Any]) -> str:
    finish = state["timeline"]["finish_date"]
    total = state["project_summary"]["total_duration_days"]
    events = len(state["timeline"]["events"])
    return f"The current plan runs for {total} working days and is projected to finish on {finish}. Logged delay events: {events}."


def explain_agent(agent: dict[str, Any]) -> str:
    if agent.get("id") == PLANNER_AGENT["id"]:
        return (
            f"{PLANNER_AGENT['name']} is the final sequencing agent. It maps extracted tasks to WBS, assigns benchmark-based durations, "
            "and rebuilds the Primavera-style order whenever an extractor runs or a delay event is logged."
        )

    return (
        f"{agent['wbs_category']} is handled by {agent['agent_name']}. It extracts BOQ scope for that package, "
        f"rewrites it into scheduler-ready subtasks, and currently holds {len(agent['latest_output'])} activities ready for sequencing."
    )


def handle_chat(state: dict[str, Any], message: str) -> str:
    lowered = message.lower()
    agent = find_agent(state, message)

    if any(phrase in lowered for phrase in ["couldn't work", "could not work", "missed today", "delay", "lost day", "couldnt work"]):
        return add_delay_event(state, message.strip(), parse_lost_days(message), TODAY.isoformat())

    if "run" in lowered and agent:
        return run_agent_logic(state, agent["id"])

    if any(keyword in lowered for keyword in ["explain", "what does", "who is", "agent"]) and agent:
        return explain_agent(agent)

    if any(keyword in lowered for keyword in ["timeline", "schedule", "finish", "completion", "recalculate"]):
        recalculate_timeline(state, "Schedule reviewed in chat")
        return summarize_timeline(state)

    return "I can explain an agent, run one of the WBS extractors, or log a delay like 'I couldn't work today, push by 1 day' and rebuild the timeline."


app = FastAPI(title="BOQ Agent Console API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

STATE = load_state()


@app.get("/health")
def health_check() -> dict[str, str]:
    return {
        "status": "ok",
        "service": "boq-agent-console-api",
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }


@app.get("/api/dashboard")
def get_dashboard() -> dict[str, Any]:
    return STATE


@app.post("/api/agents/{agent_id}/run")
def run_agent(agent_id: str) -> dict[str, Any]:
    result = run_agent_logic(STATE, agent_id)
    append_chat(STATE, "assistant", result)
    save_state(STATE)
    return STATE


@app.post("/api/boq/upload")
async def upload_boq(request: Request) -> dict[str, Any]:
    filename = request.headers.get("x-filename", "uploaded_boq.xlsx")
    extension = Path(filename).suffix.lower()
    if extension not in {".xlsx", ".xls"}:
        raise HTTPException(status_code=400, detail="Only Excel BOQ files (.xlsx or .xls) are supported.")

    file_bytes = await request.body()
    if not file_bytes:
        raise HTTPException(status_code=400, detail="Uploaded BOQ file is empty.")

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", Path(filename).name or "uploaded_boq.xlsx")
    stored_name = f"{timestamp}-{safe_name}"
    stored_path = UPLOAD_DIR / stored_name
    stored_path.write_bytes(file_bytes)

    STATE["boq_upload"] = {
        "filename": filename,
        "stored_path": str(stored_path),
        "uploaded_at": datetime.now().isoformat(timespec="seconds"),
        "status": "BOQ uploaded and ready for the project manager agent.",
    }
    STATE["project_summary"]["last_action"] = f"Uploaded BOQ file {filename}"
    append_chat(STATE, "assistant", f"BOQ file '{filename}' uploaded successfully and is ready for schedule extraction.")
    save_state(STATE)
    return STATE


@app.get("/api/timeline")
def get_timeline() -> dict[str, Any]:
    return STATE["timeline"]


@app.get("/api/exports/primavera.xlsx")
def download_primavera_export() -> FileResponse:
    refresh_primavera_export(STATE)
    return FileResponse(
        PRIMAVERA_EXPORT_FILE,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="primavera_schedule_import.xlsx",
    )


@app.post("/api/timeline/events")
def add_timeline_event(request: TimelineEventRequest) -> dict[str, Any]:
    result = add_delay_event(STATE, request.reason, max(1, request.lost_days), request.date)
    append_chat(STATE, "assistant", result)
    save_state(STATE)
    return STATE


@app.post("/api/chat")
def chat(request: ChatRequest) -> dict[str, Any]:
    append_chat(STATE, "user", request.message)
    response = handle_chat(STATE, request.message)
    append_chat(STATE, "assistant", response)
    save_state(STATE)
    return STATE
