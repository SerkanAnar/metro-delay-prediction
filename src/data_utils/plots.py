import matplotlib.pyplot as plt
import os
import pandas as pd


def plot_metro_delay_predictions(df, file_path, hindcast=False):
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.floor('30min')
    latest_date = df['timestamp'].dt.date.max()
    df = df[df['timestamp'].dt.date==latest_date]
    df = df[(df['timestamp'].dt.hour >= 8) & (df['timestamp'].dt.hour < 24)]

    df = df.sort_values('timestamp')
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    for line, g in df.groupby('line'):
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.plot(
            g['timestamp'],
            g['predicted'],
            label=f'{line} - Predicted average delay',
            marker='o'
        )

        if hindcast and 'delay_current' in g.columns:
            ax.plot(
                g['timestamp'],
                g['delay_current'],
                label=f'{line} - Actual average delay',
                linestyle='--',
                marker='^'
            )

        ax.set_xlabel('Time')
        ax.set_ylabel('Average delay (seconds)')
        ax.set_title(f'Metro delay prediction for {line}, {latest_date}')
        ax.legend(fontsize='small')

        plt.xticks(rotation=45)
        plt.tight_layout()
        os.makedirs(f"{file_path}/plots", exist_ok=True)
        if hindcast:
            plt.savefig(f"{file_path}/plots/{line.replace(' ', '_')}_hindcast.png")
        else:
            plt.savefig(f"{file_path}/plots/{line.replace(' ', '_')}_forecast.png")
            
        plt.close(fig)
