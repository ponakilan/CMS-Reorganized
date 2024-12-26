import io
import json

import pandas as pd
import plotly.graph_objects as go
from plotly.io import to_html
import matplotlib.pyplot as plt
from django.shortcuts import render
from django.http import HttpResponse, JsonResponse

pd.options.plotting.backend = "plotly"


def filter_columns(df, selected_drugs, common_columns):
    drug_columns = []
    for drug in selected_drugs:
        drug_columns.extend([
            f"{drug}_CONSULTING", f"{drug}_EDUCATION", f"{drug}_FOOD&BEVERAGE",
            f"{drug}_GENERAL", f"{drug}_SPEAKER", f"{drug}_TRAVEL",
            f"{drug}_Claims", f"{drug}_Patients",f"{drug}_OTHERS",f"{drug}_OTHERS_GENERAL"
        ])
    available_columns = [col for col in common_columns + drug_columns if col in df.columns]
    
    # Filter the DataFrame to include only the selected columns
    filtered_df = df[available_columns]
    
    # Convert the NPI column to string type
    if 'NPI' in filtered_df.columns:
        filtered_df.loc[:, 'NPI'] = filtered_df['NPI'].astype(str)
    
    # Process the 'Provider Business Mailing Address Postal Code' column
    if 'Provider Business Mailing Address Postal Code' in filtered_df.columns:
        filtered_df['Provider Business Mailing Address Postal Code'] = (
            filtered_df['Provider Business Mailing Address Postal Code']
            .astype(str)
            .str.zfill(5)
            .apply(lambda x: x.zfill(9) if len(x) > 5 else x)
            .apply(lambda x: f"{x[:5]}-{x[5:]}" if len(x) == 9 else x)
        )
    
    return filtered_df


def sum_and_sort_columns(df):
    # Keywords to filter columns
    keywords = ['CONSULTING', 'EDUCATION', 'FOOD&BEVERAGE', 'GENERAL', 'SPEAKER', 'TRAVEL', "OTHERS", "OTHERS_GENERAL",'Claims','Patients']
    
    # Calculate the total sum of columns containing the keywords
    df.loc[:, 'Total Sum'] = df.filter(regex='|'.join(keywords)).sum(axis=1)
    
    # Filter out rows where the total sum is 0
    df = df[df['Total Sum'] > 0]

    # df.loc[:, 'Zip code'] = df['Zip code'].astype(str).apply(lambda x: x.zfill(9) if len(x) in [6, 7, 8] else x.zfill(5) if len(x) == 5 else x)

    
    # Sort the DataFrame by 'Total Sum' in descending order
    df = df.sort_values(by='Total Sum', ascending=False)
    
    # Drop the 'Total Sum' column before displaying
    df = df.drop(columns=['Total Sum'])
    
    return df


def generate_visualizations(df, selected_drugs):
    patients_totals = pd.Series(dtype=float)
    claims_totals = pd.Series(dtype=float)
    inner_graph = []

    for drug in selected_drugs:
        columns = {
            'CONSULTING': f"{drug}_CONSULTING",
            'EDUCATION': f"{drug}_EDUCATION",
            'FOOD&BEVERAGE': f"{drug}_FOOD&BEVERAGE",
            'GENERAL': f"{drug}_GENERAL",
            'SPEAKER': f"{drug}_SPEAKER",
            'TRAVEL': f"{drug}_TRAVEL",
            "OTHERS": f"{drug}_OTHERS",
            "OTHERS_GENERAL": f"{drug}_OTHERS_GENERAL"
        }

        available_columns = {label: col for label, col in columns.items() if col in df.columns}
        
        if available_columns:
            totals = df[list(available_columns.values())].sum()

            # Create Pie Chart
            fig_pie = go.Figure(
                data=[go.Pie(
                    labels=totals.index,
                    values=totals.values,
                    hole=0.3
                )]
            )
            fig_pie.update_layout(
                title=f"Distribution of Payments for {drug}",
                legend_title="Payment Types"
            )
            inner_graph.append(to_html(fig_pie, full_html=False))

            # Create Bar Chart
            fig_bar = go.Figure(
                data=[go.Bar(
                    x=totals.index,
                    y=totals.values,
                    marker_color='skyblue'
                )]
            )
            fig_bar.update_layout(
                title=f"Total Payments for {drug}",
                xaxis_title="Payment Type",
                yaxis_title="Total Amount"
            )
            inner_graph.append(to_html(fig_bar, full_html=False))

    for drug in selected_drugs:
        if f"{drug}_Patients" in df.columns:
            patients_totals[drug] = df[f"{drug}_Patients"].fillna(0).sum()
        if f"{drug}_Claims" in df.columns:
            claims_totals[drug] = df[f"{drug}_Claims"].fillna(0).sum()

    def plot_bar_chart(data, title, ylabel):
        if data.empty or data.sum() == 0:
            return

        fig_bar = go.Figure(
            data=[go.Bar(
                x=data.index,
                y=data.values,
                marker_color='skyblue'
            )]
        )
        fig_bar.update_layout(
            title=title,
            xaxis_title="Drugs",
            yaxis_title=ylabel
        )
        inner_graph.append(to_html(fig_bar, full_html=False))

    if not patients_totals.empty:
        plot_bar_chart(patients_totals, "Total Patients by Drug", "Number of Patients")

    if not claims_totals.empty:
        plot_bar_chart(claims_totals, "Total Claims by Drug", "Number of Claims (in Millions)")

    return inner_graph


col_len = 27

def index(request):
    file_path = request.GET.get("file_path", "")
    df = pd.read_csv(file_path[1:])
    drug_cols = df.columns[col_len:]
    drugs = set([col.split("_")[0] for col in drug_cols])
    return render(request, "visualize.html", {"drugs": drugs, "file_path": file_path})


def filter(request):
    data = json.loads(request.body)
    file_path = data.get("file_path", "")
    selected_drugs = data.get("selected_drugs", [])
    
    df = pd.read_csv(file_path[1:])
    drug_cols = df.columns[34:]
    all_drugs = set([col.split("_")[0] for col in drug_cols])
    common_columns = list(df.columns[:col_len])

    all_drugs_option = "All"
    if all_drugs_option in selected_drugs:
        selected_drugs = all_drugs

    if selected_drugs:
        filtered_df = filter_columns(df, selected_drugs, common_columns)
        filtered_df = sum_and_sort_columns(filtered_df)
        filtered_df = filtered_df.reset_index(drop=True)
        filtered_df = filtered_df.fillna(0)

    graphs = generate_visualizations(filtered_df, selected_drugs)
    filtered = filtered_df.to_dict(orient='records')
    resp = {
        "filtered": filtered,
        "graphs": graphs
    }
    return JsonResponse(resp, safe=False)
