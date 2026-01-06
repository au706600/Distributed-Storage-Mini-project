import matplotlib.pyplot as plt

# Ingest time for N = 6 nodes and with different placement strategies and file sizes

file_size_random = [100,500,1024,10240] # File sizes in KB
y_random = [1.40, 1.54, 1.80, 7.63] # Ingest times in seconds for random placement corresponding to the file sizes

file_size_min_copy_sets = [100,500,1024,10240] # File sizes in KB
y_min_copy_sets = [1.25, 1.56, 1.97, 7.59] # Ingest times in seconds for min copy sets corresponding to the file sizes

file_size_buddy_approach = [100,500,1024,10240] # File sizes in KB
y_buddy_approach = [1.26, 1.47, 1.79, 7.42] # Ingest times in seconds for buddy approach corresponding to the file sizes

plt.figure()
plt.title(' Ingest time for different placement strategies with different filesizes for N = 6 nodes')
plt.xlabel('File size (KB)')
plt.ylabel('Ingest time (s)')
plt.plot(file_size_random, y_random, label = "random placement")
plt.plot(file_size_min_copy_sets, y_min_copy_sets, label = "min copy sets")
plt.plot(file_size_buddy_approach, y_buddy_approach, label = "buddy approach")
plt.legend()

#-------------------------------------------------------------------------------

# Ingest time for N = 12 nodes and with different placement strategies and file sizes

file_size_random = [100,500,1024,10240] # File sizes in KB
y_random = [1.23, 1.51, 1.95, 10.33] # Ingest times in seconds for random placement corresponding to the file sizes

file_size_min_copy_sets = [100,500,1024,10240] # File sizes in KB
y_min_copy_sets = [1.32, 1.53, 1.89, 11.10] # Ingest times in seconds for min copy sets corresponding to the file sizes

file_size_buddy_approach = [100,500,1024,10240] # File sizes in KB
y_buddy_approach = [1.26, 1.51, 2.04, 9.97] # Ingest times in seconds for buddy approach corresponding to the file sizes

plt.figure()
plt.title(' Ingest time for different placement strategies with different filesizes for N = 12 nodes')
plt.xlabel('File size (KB)')
plt.ylabel('Ingest time (s)')
plt.plot(file_size_random, y_random, label = "random placement")
plt.plot(file_size_min_copy_sets, y_min_copy_sets, label = "min copy sets")
plt.plot(file_size_buddy_approach, y_buddy_approach, label = "buddy approach")
plt.legend()


#--------------------------------------------------------------------------------------

# Ingest time for N = 24 nodes and with different placement strategies and file sizes

file_size_random = [100,500,1024,10240] # File sizes in KB
y_random = [1.95, 2.86, 3.49, 8.91] # Ingest times in seconds for random placement corresponding to the file sizes

file_size_min_copy_sets = [100,500,1024,10240] # File sizes in KB
y_min_copy_sets = [2.37, 1.86, 3.48, 9.77] # Ingest times in seconds for min copy sets corresponding to the file sizes

file_size_buddy_approach = [100,500,1024,10240] # File sizes in KB
y_buddy_approach = [2.42, 2.67, 2.92, 8.47] # Ingest times in seconds for buddy approach corresponding to the file sizes

plt.figure()
plt.title(' Ingest time for different placement strategies with different filesizes for N = 24 nodes')
plt.xlabel('File size (KB)')
plt.ylabel('Ingest time (s)')
plt.plot(file_size_random, y_random, label = "random placement")
plt.plot(file_size_min_copy_sets, y_min_copy_sets, label = "min copy sets")
plt.plot(file_size_buddy_approach, y_buddy_approach, label = "buddy approach")
plt.legend()
plt.show()