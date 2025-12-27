from typing import Optional
from core.cases.case_validator import validate_case


class CaseValidationRunner:
    """
    可选的 Case 校验执行器
    - 不绑定 Engine
    - 由配置 / CLI / 调试模式触发
    """

    def __init__(self, *, enabled: bool, case_path: Optional[str] = None):
        self.enabled = enabled
        self.case_path = case_path

    def run(
        self,
        *,
        gate_final: str,
        summary_code: str,
        structure: dict,
        report_text: str,
    ) -> None:
        if not self.enabled or not self.case_path:
            return

        validate_case(
            case_path=self.case_path,
            gate_final=gate_final,
            summary_code=summary_code,
            structure=structure,
            report_text=report_text,
        )
