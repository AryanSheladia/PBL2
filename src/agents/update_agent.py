import requests
import json
from typing import List, Dict

LM_STUDIO_URL = "http://127.0.0.1:1234/v1/chat/completions"
MODEL_NAME = "qwen2.5-7b-instruct"


def normalize(s):
    return s.strip().lower()


class UpdateAgent:

    def __init__(self):
        self._check_connection()

    def _check_connection(self):
        try:
            r = requests.get("http://127.0.0.1:1234/v1/models")
            if r.status_code == 200:
                print("🟢 LM Studio Connected")
        except:
            print("❌ LM Studio not reachable")

    def _clean_json(self, raw):
        raw = raw.strip()

        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]

        try:
            return json.loads(raw.strip())
        except:
            print("⚠️ JSON parse failed")
            return {}

    def _call_llm(self, prompt):
        try:
            res = requests.post(
                LM_STUDIO_URL,
                json={
                    "model": MODEL_NAME,
                    "messages": [
                        {"role": "system", "content": "Return ONLY JSON"},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.2
                }
            )

            raw = res.json()["choices"][0]["message"]["content"]
            return self._clean_json(raw)

        except Exception as e:
            print("❌ LLM Error:", e)
            return {}

    def _build_prompt(self, change, content):

        return f"""
CHANGE:
Type: {change['change_type']}
Old: {change.get('old_text', '')}
New: {change.get('new_text', '')}

TARGET SECTION:
{content}

TASK:
Decide:
modify / delete / keep

RETURN JSON:
{{
  "decision": "",
  "updated_text": "",
  "reasoning": "",
  "confidence": 0.xx
}}
"""

    def generate_updates(self, changes, dependency_map, section_lookup):

        updates = []

        for change in changes:
            impacted = dependency_map.get(change["section_id"], [])

            for dep in impacted:

                target_id = dep["section_id"]

                if target_id == change["section_id"]:
                    continue

                content = section_lookup.get(normalize(target_id), "")
                if not content:
                    continue

                # 🔴 DELETE PROPAGATION → REMOVE SECTION
                if change["change_type"] == "deleted":
                    updates.append({
                        "target_section_id": target_id,
                        "decision": "delete",
                        "old_text": content,
                        "updated_text": "",
                        "reasoning": "Dependency removed → section deleted",
                        "confidence": 0.95
                    })
                    continue

                # 🟢 LLM FOR MODIFY
                result = self._call_llm(self._build_prompt(change, content))

                if not result:
                    continue

                updates.append({
                    "target_section_id": target_id,
                    "decision": result.get("decision", "modify"),
                    "old_text": content,
                    "updated_text": result.get("updated_text", ""),
                    "reasoning": result.get("reasoning", ""),
                    "confidence": result.get("confidence", 0.0)
                })

        return updates