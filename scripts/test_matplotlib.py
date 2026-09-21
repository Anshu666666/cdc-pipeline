import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(6, 4))
ax.plot([1, 2, 3], [4, 5, 6])
fig.savefig('test_plot.png')
print("Matplotlib plot saved successfully!")
