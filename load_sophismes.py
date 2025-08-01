import yaml

SOPHISMES_FILE = "sophismes.yaml"

def load_sophismes():
    with open(SOPHISMES_FILE, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def print_sophismes_summary(sophismes):
    print(f"\n🧠 {len(sophismes)} sophismes chargés :\n")
    for s in sophismes:
        print(f"- [{s['category']}] {s['name']} : {s['description']}")

if __name__ == "__main__":
    sophismes = load_sophismes()
    print_sophismes_summary(sophismes)
