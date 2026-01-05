import matplotlib.dates as mdates
from matplotlib.patches import Patch
import matplotlib.pyplot as plt
import os
import pandas as pd


def plot_metro_delay_predictions(df:pd.DataFrame, file_path, hindcast=False):
    df = df.copy()
    df = df.sort_values('timestamp')
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.floor('30min')
    df = df.drop_duplicates(subset=['line', 'timestamp'])
    latest_date = df['timestamp'].dt.date.max()
    df = df[df['timestamp'].dt.date==latest_date]
    df = df[(df['timestamp'].dt.hour >= 8) & (df['timestamp'].dt.hour < 24)]

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    colors = ['green', 'yellow', 'orange', 'red']
    labels = ['On time', 'Minor delay', 'Delay', 'Severe delay']
    ranges = [(-120, 0), (0, 60), (60, 180), (180, 500)]

    for line, g in df.groupby('line'):
        fig, ax = plt.subplots(figsize=(10, 6))
        for color, (start, end) in zip(colors, ranges):
            ax.axhspan(start, end, color=color, alpha=0.25)
        ax.plot(
            g['timestamp'] + pd.Timedelta(minutes=30),
            g['prediction'],
            label=f'{line} - Predicted average delay',
            marker='o'
        )

        if hindcast and 'delay_hind' in g.columns:
            ax.plot(
                g['timestamp'],
                g['delay_hind'],
                label=f'{line} - Actual average delay',
                linestyle='--',
                marker='^'
            )

        ax.set_xlabel('Time')
        ax.set_ylabel('Average delay (seconds)')
        if hindcast:
            ax.set_title(f'Metro delay hindcast for {line}, {latest_date}')
        else:
            ax.set_title(f'Metro delay forecast for {line}, {latest_date}')
        line_legend = ax.legend(loc='upper left', fontsize='small')
        patches = [Patch(color=colors[i], label=f"{labels[i]} ({ranges[i][0]}-{ranges[i][1]} s)") for i in range(len(colors))]
        band_legend = ax.legend(handles=patches, loc='upper right', title='Delay level', fontsize='x-small')
        ax.add_artist(line_legend)
        
        ax.set_ylim(-120, 500)
        date = g['timestamp'].dt.normalize().iloc[0]
        start = date + pd.Timedelta(hours=7)
        end = date + pd.Timedelta(hours=24)
        ax.set_xlim(start, end)

        plt.xticks(rotation=45)
        plt.tight_layout()
        ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=30))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        os.makedirs(f"{file_path}", exist_ok=True)
        if hindcast:
            plt.savefig(f"{file_path}/{line.replace(' ', '_')}_hindcast.png")
        else:
            plt.savefig(f"{file_path}/{line.replace(' ', '_')}_forecast.png")
            
        plt.close(fig)
