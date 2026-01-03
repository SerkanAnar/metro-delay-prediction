import matplotlib.pyplot as plt
import pandas as pd


def plot_metro_delay_predictions(df, file_path, hindcast=False):
    fig, ax = plt.subplots(figsize=(10, 6))

    df = df.sort_values('timestamp')
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    for line, g in df.groupby('line'):
        ax.plot(
            g['timestamp'],
            g['predicted_delay'],
            label=f'{line} - predicted',
            marker='o'
        )

        if hindcast and 'actual_delay' in g.columns:
            ax.plot(
                g['timestamp'],
                g['actual_delay'],
                label=f'{line} - actual',
                linestyle='--',
                marker='^'
            )

    ax.set_xlabel('Time')
    ax.set_ylabel('Average delay (seconds)')
    ax.set_title('Metro delay prediction per line')
    ax.legend(fontsize='small')

    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(file_path)
    return plt