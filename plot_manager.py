import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('data_giri.csv', index_col=0)

df.plot.scatter(x = 'Sviluppo (km)', y = 'Dislivello tot (m)')

for i, txt in enumerate(df['Name']):
    plt.annotate(txt, (df['Sviluppo (km)'][i], df['Dislivello tot (m)'][i]))

plt.title("Giri")
plt.show()