# metrics_process/metrics_modules/monitoring/reports.py
from typing import List, Dict, Any

class Reports:
    """
    Uma classe de modelo de dados simples que serve como um contentor 
    para os resultados das validações durante uma execução.
    """
    def __init__(self):
        self.reports: List[Dict[str, Any]] = []

    def add_report(self, report: Dict[str, Any]):
        self.reports.append(report)

    def get_reports(self) -> List[Dict[str, Any]]:
        return self.reports
    
    def clear_reports(self):
        self.reports = []

    def generate_text_report(self) -> str: # <<< Nome corrigido
        """Gera um resumo em texto simples para os logs."""
        if not self.reports:
            return "Nenhuma métrica foi executada ou todas falharam antes de gerar relatório."

        report_str = "\n--- HAL-9000: Resumo da Execução ---\n"
        for report in self.reports:
            content = report.get("content", {})
            status = content.get("status", "N/A")
            metric = content.get("metric", "N/A")
            column = content.get("column", "N/A")
            value = content.get("value", "N/A")
            report_str += f"[{status}] Métrica '{metric}' na coluna '{column}' com valor: {value}\n"
        
        report_str += "---------------------------------------\n"
        return report_str