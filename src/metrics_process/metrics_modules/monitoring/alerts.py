class Alerts:
    def __init__(self):
        self.alerts = []

    def add_alert(self, alert):
        self.alerts.append(alert)

    def get_alerts(self):
        return self.alerts
    
    def clear_alerts(self):
        self.alerts = []
        
    def save_alerts_to_file(self, file_path):
        with open(file_path, 'w') as f:
            for alert in self.alerts:
                f.write(f"{alert}\n")
    
    def consolidate_alerts(self):
        consolidated = {}
        for alert in self.alerts:
            key = alert.get('type', 'general')
            if key not in consolidated:
                consolidated[key] = []
            consolidated[key].append(alert)
        return consolidated
    
    def alert_summary(self):
        summary = {}
        for alert in self.alerts:
            key = alert.get('type', 'general')
            summary[key] = summary.get(key, 0) + 1
        return summary
    
    