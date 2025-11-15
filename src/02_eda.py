import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
PROJECT_ROOT = Path(__file__).resolve().parents[1]
ANALYZE_DIR = PROJECT_ROOT/"data" / "processed"
RESULTS_DIR = PROJECT_ROOT / "results"

def main():
    df= (pd.read_csv(ANALYZE_DIR  / "capital_by_position_team_year.csv" )
                                    .query('year == 2017')
                                    
                                    )

    pos = "VDU"

    #get just vdu
    df_pos = df[df["position"] == pos].copy()

    #get totals by each team
    team_totals = df.groupby("team")["cap_pct_team"].sum().rename("cap_total")
    merged = df_pos.merge(team_totals, on="team", how="left")
    #get percentage spent on other positions
    merged["cap_other"] = merged["cap_total"] - merged["cap_pct_team"]


    merged = merged.sort_values("team")

    fig, ax = plt.subplots(figsize=(12, 6))

    ax.bar(
        merged["team"],
        merged["cap_pct_team"],
        label=f"{pos} cap %",
    )

    ax.bar(
        merged["team"],
        merged["cap_other"],
        bottom=merged["cap_pct_team"],
        label="All other positions",
    )

    ax.set_ylabel("Cap Percentage")
    ax.set_title(f"Cap Allocation by Team in 2017: {pos} vs All Other Positions")
    ax.set_xticklabels(merged["team"], rotation=45, ha="right")
    ax.legend()

    plt.tight_layout()

    output_path = RESULTS_DIR / "cap_allocation_2017_vdu.png"
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.show()
    plt.close()

if __name__ == "__main__":
    main()