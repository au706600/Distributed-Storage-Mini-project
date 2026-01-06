import matplotlib.pyplot as plt

# Download time for N = 6 nodes and with different placement strategies and file sizes

file_size_random = [100,500,1024,10240] # File sizes in KB
y_random = [0.63, 0.75, 0.80, 2.73]


file_size_min_copy_sets = [100,500,1024,10240] # File sizes in KB
y_min_copy_sets = [0.65, 0.74, 1.13, 5.13]


file_size_buddy_approach = [100,500,1024,10240] # File sizes in KB
y_buddy_approach = [0.66, 0.70, 0.79, 2.78]

plt.figure()
plt.title(' Download time for different placement strategies with different filesizes for N = 6 nodes')
plt.xlabel('File size (KB)')
plt.ylabel('Download time (s)')
plt.plot(file_size_random, y_random, label = "random placement")
plt.plot(file_size_min_copy_sets, y_min_copy_sets, label = "min copy sets")
plt.plot(file_size_buddy_approach, y_buddy_approach, label = "buddy approach")
plt.legend()

#-------------------------------------------------------------------------------

# Download time for N = 12 nodes and with different placement strategies and file sizes

file_size_random = [100,500,1024,10240] # File sizes in KB
y_random = [0.65, 0.76, 1.06, 4.76]


file_size_min_copy_sets = [100,500,1024,10240] # File sizes in KB
y_min_copy_sets = [0.63, 0.69, 0.85, 4.63]


file_size_buddy_approach = [100,500,1024,10240] # File sizes in KB
y_buddy_approach = [0.65, 0.75, 1.03, 5.73]

plt.figure()
plt.title(' Download time for different placement strategies with different filesizes for N = 12 nodes')
plt.xlabel('File size (KB)')
plt.ylabel('Download time (s)')
plt.plot(file_size_random, y_random, label = "random placement")
plt.plot(file_size_min_copy_sets, y_min_copy_sets, label = "min copy sets")
plt.plot(file_size_buddy_approach, y_buddy_approach, label = "buddy approach")
plt.legend()

#--------------------------------------------------------------------------------------

# Download time for N = 24 nodes and with different placement strategies and file sizes

file_size_random = [100,500,1024,10240] # File sizes in KB
y_random = [0.71, 0.97, 1.56, 4.66]


file_size_min_copy_sets = [100,500,1024,10240] # File sizes in KB
y_min_copy_sets = [0.77, 0.85, 0.93, 4.45]


file_size_buddy_approach = [100,500,1024,10240] # File sizes in KB
y_buddy_approach = [0.77, 1.24, 0.93, 2.86]

plt.figure()
plt.title(' Download time for different placement strategies with different filesizes for N = 24 nodes')
plt.xlabel('File size (KB)')
plt.ylabel('Download time (s)')
plt.plot(file_size_random, y_random, label = "random placement")
plt.plot(file_size_min_copy_sets, y_min_copy_sets, label = "min copy sets")
plt.plot(file_size_buddy_approach, y_buddy_approach, label = "buddy approach")
plt.legend()
plt.show()