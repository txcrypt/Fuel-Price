"""
advanced_ai.py - Gemini-backed advanced market analysis helpers.

The backend keeps the Gemini API key server-side and sends the model compact,
curated evidence packs rather than raw database dumps.
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


class AdvancedAIService:
    """Small wrapper around the Google GenAI SDK with deterministic fallbacks."""

    def __init__(self) -> None:
        self.api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        self.model = os.getenv("GEMINI_MODEL", "gemini-3.5-flash")
        self._client = None
        self._disabled_reason = ""

        if not self.api_key:
            self._disabled_reason = "GEMINI_API_KEY is not configured."

    @property
    def available(self) -> bool:
        return bool(self.api_key) and self._disabled_reason == ""

    @property
    def disabled_reason(self) -> str:
        return self._disabled_reason or "Gemini analysis is unavailable."

    def ask(self, question: str, evidence: dict[str, Any], history: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        prompt = {
            "task": "Answer a trusted fuel-market analyst question in plain English.",
            "question": question,
            "conversation_history": (history or [])[-8:],
            "evidence": evidence,
            "rules": [
                "Use only the supplied evidence.",
                "Reference specific provided values when making claims.",
                "If evidence is missing, say what is missing.",
                "Keep the answer concise and decision-oriented.",
            ],
        }
        fallback = self._fallback_answer(question, evidence)
        text = self._generate_text(prompt, _ANALYST_SYSTEM_PROMPT, temperature=0.2)
        return {
            "answer": text or fallback,
            "disabled": text is None,
            "message": self.disabled_reason if text is None else "",
        }

    def briefing(self, evidence: dict[str, Any]) -> dict[str, Any]:
        prompt = {
            "task": "Generate a morning executive fuel-market briefing.",
            "evidence": evidence,
            "response_schema": {
                "title": "string",
                "summary": ["paragraph 1", "paragraph 2"],
                "action": "string",
                "risks": ["string"],
            },
            "rules": [
                "Return JSON only.",
                "Use two short summary paragraphs.",
                "Mention only metrics present in evidence.",
                "Do not invent dates, prices, or causes.",
            ],
        }
        text = self._generate_text(prompt, _ANALYST_SYSTEM_PROMPT, temperature=0.2)
        data = _parse_json_object(text) if text else None
        if not isinstance(data, dict):
            data = self._fallback_briefing(evidence)
            data["disabled"] = True
            data["message"] = self.disabled_reason
        else:
            data["disabled"] = False
            data["message"] = ""
        return data

    def shock(self, scenario: str, evidence: dict[str, Any]) -> dict[str, Any]:
        parsed = self.parse_shock_variables(scenario, evidence)
        forecast = self.forecast_shock_impact(parsed, evidence)
        prompt = {
            "task": "Explain a deterministic shock simulation result.",
            "scenario": scenario,
            "parsed_variables": parsed,
            "forecast_impact": forecast,
            "evidence": evidence,
            "rules": [
                "Explain the result in plain English.",
                "Do not change parsed_variables or forecast_impact.",
                "Reference provided metrics only.",
                "Keep it to one short paragraph.",
            ],
        }
        text = self._generate_text(prompt, _ANALYST_SYSTEM_PROMPT, temperature=0.2)
        return {
            "parsed_variables": parsed,
            "forecast_impact": forecast,
            "explanation": text or self._fallback_shock_explanation(scenario, parsed, forecast),
            "disabled": text is None,
            "message": self.disabled_reason if text is None else "",
        }

    def parse_shock_variables(self, scenario: str, evidence: dict[str, Any]) -> dict[str, Any]:
        prompt = {
            "task": "Convert a natural-language market shock into strict simulation variables.",
            "scenario": scenario,
            "evidence": evidence,
            "response_schema": {
                "aud_usd_delta": "number, AUD/USD absolute change, e.g. -0.05",
                "brent_usd_delta": "number, USD per barrel change",
                "supply_risk_level": "low|medium|high",
                "demand_risk_level": "low|medium|high",
                "confidence": "number 0..1",
            },
            "rules": [
                "Return JSON only.",
                "Use 0 for omitted numeric variables.",
                "Do not include markdown.",
            ],
        }
        text = self._generate_text(prompt, _ANALYST_SYSTEM_PROMPT, temperature=0.0)
        data = _parse_json_object(text) if text else None
        if not isinstance(data, dict):
            data = self._local_parse_shock(scenario)
        return _clamp_shock_variables(data)

    def forecast_shock_impact(self, variables: dict[str, Any], evidence: dict[str, Any]) -> dict[str, Any]:
        ticker = evidence.get("market_status", {}).get("ticker", {})
        current = evidence.get("market_status", {})
        current_tgp = _safe_float(ticker.get("tgp"), 165.0)
        current_avg = _safe_float(current.get("current_avg"), current_tgp + 12.0)
        current_fx = _safe_float(ticker.get("fx"), 0.65)
        current_oil = _safe_float(ticker.get("oil"), 75.0)

        fx_after = max(0.35, current_fx + _safe_float(variables.get("aud_usd_delta"), 0.0))
        oil_after = max(20.0, current_oil + _safe_float(variables.get("brent_usd_delta"), 0.0))

        barrels_to_litres = 158.987
        oil_cpl_now = current_oil / barrels_to_litres / max(current_fx, 0.35) * 100
        oil_cpl_after = oil_after / barrels_to_litres / fx_after * 100
        wholesale_delta = oil_cpl_after - oil_cpl_now

        supply_risk = str(variables.get("supply_risk_level", "low")).lower()
        demand_risk = str(variables.get("demand_risk_level", "low")).lower()
        supply_premium = {"low": 0.0, "medium": 2.0, "high": 5.0}.get(supply_risk, 0.0)
        demand_discount = {"low": 0.0, "medium": 1.5, "high": 4.0}.get(demand_risk, 0.0)

        tgp_delta = round(wholesale_delta + supply_premium - demand_discount, 2)
        retail_pass_through = round(tgp_delta * 0.8, 2)
        projected_tgp = round(current_tgp + tgp_delta, 2)
        projected_retail = round(current_avg + retail_pass_through, 2)
        current_margin = round(current_avg - current_tgp, 2)
        projected_margin = round(projected_retail - projected_tgp, 2)

        return {
            "current_tgp_cpl": round(current_tgp, 2),
            "projected_tgp_cpl": projected_tgp,
            "tgp_delta_cpl": tgp_delta,
            "current_retail_avg_cpl": round(current_avg, 2),
            "projected_retail_avg_cpl": projected_retail,
            "retail_delta_cpl": retail_pass_through,
            "current_margin_cpl": current_margin,
            "projected_margin_cpl": projected_margin,
            "fx_after": round(fx_after, 4),
            "brent_after_usd": round(oil_after, 2),
        }

    def _generate_text(self, payload: dict[str, Any], system_instruction: str, temperature: float) -> str | None:
        if not self.api_key:
            return None

        try:
            client = self._get_client()
            prompt = json.dumps(payload, ensure_ascii=False, default=str)

            if hasattr(client, "interactions"):
                interaction = client.interactions.create(
                    model=self.model,
                    system_instruction=system_instruction,
                    input=prompt,
                    generation_config={"temperature": temperature},
                )
                return getattr(interaction, "output_text", None)

            response = client.models.generate_content(
                model=self.model,
                contents=prompt,
                config={
                    "system_instruction": system_instruction,
                    "temperature": temperature,
                },
            )
            return getattr(response, "text", None)
        except Exception as exc:
            logger.warning("Gemini request failed: %s", exc)
            self._disabled_reason = f"Gemini request failed: {exc}"
            return None

    def _get_client(self):
        if self._client is not None:
            return self._client
        try:
            from google import genai

            self._client = genai.Client(api_key=self.api_key)
            return self._client
        except Exception as exc:
            logger.warning("Google GenAI SDK unavailable: %s", exc)
            self._disabled_reason = f"Google GenAI SDK unavailable: {exc}"
            raise

    @staticmethod
    def _local_parse_shock(scenario: str) -> dict[str, Any]:
        text = scenario.lower()
        aud_delta = 0.0
        brent_delta = 0.0

        aud_match = re.search(r"(?:aud|dollar)[^.\n]*?(drops?|falls?|down|weakens?|loses?)[^0-9-]*(\d+(?:\.\d+)?)\s*(?:c|cent|cents)", text)
        if aud_match:
            aud_delta = -float(aud_match.group(2)) / 100
        else:
            aud_match = re.search(r"(?:aud|dollar)[^.\n]*?(rises?|jumps?|up|strengthens?|gains?)[^0-9-]*(\d+(?:\.\d+)?)\s*(?:c|cent|cents)", text)
            if aud_match:
                aud_delta = float(aud_match.group(2)) / 100

        brent_match = re.search(r"(?:brent|oil|crude)[^.\n]*?(jumps?|rises?|up|increases?)[^0-9-]*(?:us\$|\$)?(\d+(?:\.\d+)?)", text)
        if brent_match:
            brent_delta = float(brent_match.group(2))
        else:
            brent_match = re.search(r"(?:brent|oil|crude)[^.\n]*?(drops?|falls?|down|decreases?)[^0-9-]*(?:us\$|\$)?(\d+(?:\.\d+)?)", text)
            if brent_match:
                brent_delta = -float(brent_match.group(2))

        supply_risk = "low"
        if any(term in text for term in ["ban", "war", "sanction", "embargo", "export", "blockade"]):
            supply_risk = "high"
        elif any(term in text for term in ["shortage", "refinery", "strike", "disruption"]):
            supply_risk = "medium"

        demand_risk = "low"
        if any(term in text for term in ["recession", "lockdown", "demand collapse"]):
            demand_risk = "high"
        elif any(term in text for term in ["slowdown", "holiday demand", "demand weakens"]):
            demand_risk = "medium"

        return {
            "aud_usd_delta": aud_delta,
            "brent_usd_delta": brent_delta,
            "supply_risk_level": supply_risk,
            "demand_risk_level": demand_risk,
            "confidence": 0.55,
        }

    @staticmethod
    def _fallback_answer(question: str, evidence: dict[str, Any]) -> str:
        status = evidence.get("market_status", {})
        ticker = status.get("ticker", {})
        cycle = status.get("cycle", {})
        return (
            f"Gemini is not configured, so this is a local summary. Current average is "
            f"{status.get('current_avg', 'unknown')} cpl, TGP is {ticker.get('tgp', 'unknown')} cpl, "
            f"and the detected cycle phase is {cycle.get('phase', 'UNKNOWN')}. "
            f"The question was: {question}"
        )

    @staticmethod
    def _fallback_briefing(evidence: dict[str, Any]) -> dict[str, Any]:
        status = evidence.get("market_status", {})
        ticker = status.get("ticker", {})
        return {
            "title": "Morning Fuel Briefing",
            "summary": [
                f"Current average retail price is {status.get('current_avg', 'unknown')} cpl, with TGP at {ticker.get('tgp', 'unknown')} cpl.",
                f"Advice is currently {status.get('advice', 'Check App')} and hike probability is {status.get('hike_probability', 0)}%.",
            ],
            "action": status.get("savings_insight", "Check the dashboard before filling up."),
            "risks": ["Gemini is not configured; this is a deterministic local briefing."],
        }

    @staticmethod
    def _fallback_shock_explanation(scenario: str, variables: dict[str, Any], forecast: dict[str, Any]) -> str:
        return (
            f"For '{scenario}', the local parser maps the shock to AUD/USD delta "
            f"{variables.get('aud_usd_delta')} and Brent delta {variables.get('brent_usd_delta')}. "
            f"That implies a TGP move of {forecast.get('tgp_delta_cpl')} cpl and a retail move of "
            f"{forecast.get('retail_delta_cpl')} cpl."
        )


def _parse_json_object(text: str | None) -> dict[str, Any] | None:
    if not text:
        return None
    cleaned = text.strip()
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start >= 0 and end > start:
        cleaned = cleaned[start:end + 1]
    try:
        return json.loads(cleaned)
    except Exception:
        return None


def _clamp_shock_variables(data: dict[str, Any]) -> dict[str, Any]:
    risk_values = {"low", "medium", "high"}
    return {
        "aud_usd_delta": round(max(-0.15, min(0.15, _safe_float(data.get("aud_usd_delta"), 0.0))), 4),
        "brent_usd_delta": round(max(-40.0, min(40.0, _safe_float(data.get("brent_usd_delta"), 0.0))), 2),
        "supply_risk_level": str(data.get("supply_risk_level", "low")).lower()
        if str(data.get("supply_risk_level", "low")).lower() in risk_values else "low",
        "demand_risk_level": str(data.get("demand_risk_level", "low")).lower()
        if str(data.get("demand_risk_level", "low")).lower() in risk_values else "low",
        "confidence": round(max(0.0, min(1.0, _safe_float(data.get("confidence"), 0.5))), 2),
    }


def _safe_float(value: Any, fallback: float) -> float:
    try:
        if value is None:
            return fallback
        return float(value)
    except (TypeError, ValueError):
        return fallback


_ANALYST_SYSTEM_PROMPT = (
    "You are the Advanced Analyst inside an Australian fuel intelligence dashboard. "
    "You explain ULP market movements using only the evidence supplied by the backend. "
    "Never claim access to raw databases, private APIs, or external facts unless they are in the evidence pack. "
    "Be concise, numerical, and action-oriented."
)
