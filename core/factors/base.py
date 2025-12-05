class FactorResult:
    """
    统一因子结果结构（支持 V11.8 / V12 全体系）
    """

    __slots__ = (
        "name", "score", "level", "details",
        "signal", "raw", "factor_obj", "report_block"
    )

    def __init__(
        self,
        name: str,
        score: float,
        level: str = None,
        details: dict | None = None,
        raw: dict | None = None,
        factor_obj=None,
        report_block: str | None = None,
    ):
        self.name = name
        self.score = score
        self.level = level
        self.details = details or {}

        # ★ 自动信号：统一评分体系依赖
        self.signal = self._compute_signal(score)

        # ★ 保留原始输入数据（旧 scorer 协议需要）
        self.raw = raw or {}

        # ★ 因子对象指针（用于 reporter → explain / report_block）
        self.factor_obj = factor_obj

        # ★ 因子生成的报告文本（新版本因子使用）
        self.report_block = report_block

    # 统一信号生成
    def _compute_signal(self, score: float) -> int:
        if score >= 60:
            return 1
        if score > 45:
            return 0
        return -1

    def __repr__(self):
        return (
            f"FactorResult(name={self.name}, score={self.score}, "
            f"level={self.level}, signal={self.signal})"
        )
