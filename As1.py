from mpi4py import MPI
import json
import re
import os
from collections import Counter


def find_offsets(filename, number_chunk):
    offset_list = []
    with open(filename, mode='rb') as f:
        count = 0
        offset_str = ""
        offset_str += "0 "
        chunk_size = 1024
        chunk = f.read(chunk_size)
        pos = 0
        while chunk:
            end_pos = pos + chunk_size
            if b'"_id": "' in chunk:
                idx = chunk.index(b'"_id": "')
                offset = pos + idx + len(b'"_id": "')
                offset_list.append(offset)
                count += 1
            chunk = f.read(chunk_size)
            pos = end_pos

    # Calculate the length of the list and the size of each part
    list_len = count
    part_size = list_len // number_chunk

    # Use list slicing to create the parts
    parts = [offset_list[i:i + part_size] for i in range(0, list_len, part_size)]
    offset_chunk = []
    # create the  chunk with start_po and end_po
    for i in range(number_chunk):
        if i == 0:  # add head
            offset_chunk.append((parts[i][0], parts[i][-1]))
        elif i == number_chunk - 1:  # add the tail
            offset_chunk.append((parts[i - 1][-1], os.stat(filename).st_size - 1))
        else:
            offset_chunk.append((parts[i - 1][-1], parts[i][-1]))
    return offset_chunk



def format_name(name_read, cities):
    """
    format the full_name to "ng___ form"
    :param name_read:
    :return: formatted full_name
    """
    name = name_read.split(',')
    place_name = name[0].strip().lower()
    state_name = ''.join(name[1:]).strip()
    name_states = {
        ' Victoria': 'vic.',
        ' Melbourne': 'vic.',
        ' Sydney': 'nsw',
        ' New South Wales': 'nsw',
        ' Queensland': 'qld',
        ' Brisbane': 'qld',
        ' South Australia': 'sa',
        ' Adelaide': 'sa',
        ' Western Australia': 'wa',
        ' Perth': 'wa',
        ' Tasmania': 'tas.',
        ' Hobart': 'tas.',
        ' Northern Territory': 'nt',
        ' Darwin': 'nt',
        ' Canberra': 'act',
        ' Australian Capital Territory': 'act'
    }

    try:
        return cities.get(place_name)

    except KeyError:
        try:
            return cities.get(place_name + " (" + name_states[state_name] + ")")
        except KeyError:
            return False


def analyse(authorid, fullname):
    author_id = authorid
    full_name = fullname

    if format_name(full_name, city_dict):
        city = format_name(full_name, city_dict)
        # count city
        city_count[city] += 1
        # count author

        if not author_city_count.get(author_id):
            author_city_count[author_id] = [0, 0, 0, 0, 0, 0, 0, 0, 0]
        idx = int(city[0]) - 1
        author_city_count[author_id][idx] += 1

# create communicator
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


# Open file, store the chunk's offset.
filename = "bigTwitter.json"
chunk_no = 300
chunk_list = find_offsets(filename, chunk_no)
if rank == 0:
    print(f"\n{chunk_no} chunks \n")


# store the cities
city_dict = {}
# store the city count
city_count = {"1gsyd": 0, "2gmel": 0, "5gper": 0,
              "3gbri": 0, "4gade": 0, "8acte": 0,
              "6ghob": 0, "7gdar": 0, "9oter": 0}
# store the author count
author_count = []
# store author's city
author_city_count = {}
with open('sal.json', 'r', encoding="utf-8") as sal_file:
    sal = json.load(sal_file)
    for location in sal.keys():
        if sal.get(location).get('gcc')[1] == 'g' or sal.get(location).get('gcc')[1] == 'o' or \
                sal.get(location).get('gcc')[1] == 'a':
            city_dict[location] = sal.get(location).get('gcc')

# assign scatter list(used for scattering)
scatter_list = [[] for _ in range(size)]

for i in range(len(chunk_list)):
    idx = i % len(scatter_list)
    scatter_list[idx].append(chunk_list[i])

# scatter to cores
offset_list = comm.scatter(scatter_list)

for offset in offset_list:

    with open(filename, 'rb') as f:
        # Set the file pointer to the start position of the chunk
        f.seek(offset[0])

        # Read the chunk of data from the file
        chunk_data = f.read(offset[1] - offset[0])

    # Process the data using regular expression
    pattern = r'"author_id"\s*:\s*"(\d+)"|"full_name"\s*:\s*"([^"]+)"'

    author_id = None
    full_name = None

    for match in re.finditer(pattern, chunk_data.decode('utf-8', 'strict')):
        if match.group(1) is not None:  # if find the patten"author_id"
            author_id = match.group(1)
            author_count.append(author_id)
        elif author_id is not None and full_name is None:  # if find the patten full_name
            full_name = match.group(2)
            analyse(author_id, full_name)
            author_id = None
            full_name = None


def merge_cities(d1, d2):
    for key, value in d2.items():
        if d1.get(key):
            d1[key] += value
        else:
            d1[key] = value
    return d1


def merge_author_city(d1, d2):
    for key, value in d2.items():
        if d1.get(key):
            for i in range(9):
                d1[key][i] += value[i]
        else:
            d1[key] = value
    return d1


gather_author = comm.gather(author_count, root=0)
gather_city = comm.reduce(city_count, op=merge_cities, root=0)
author_city = comm.reduce(author_city_count, op=merge_author_city, root=0)

if rank == 0:

    # Task 1
    my_list = []
    for i in range(len(gather_author)):
        my_list += gather_author[i]
    freq_dict = Counter(my_list)

    # Print the frequency of each item
    print("Rank Author Id Number of Tweets Made")
    ranking = 0
    for item, freq in freq_dict.most_common(10):
        ranking += 1
        print(f"#{ranking}    {item}   {freq}")

    # Task2

    # Create a dictionary to map city codes to full city names
    city_names = {'1gsyd': 'Greater Sydney', '2gmel': 'Greater Melbourne', '3gbri': 'Greater Brisbane',
                  '4gade': 'Greater Adelaide', '5gper': 'Greater Perth', '6ghob': 'Greater Hobart',
                  "7gdar": 'Greater Darwin', "8acte": "Australian Capital Territory",
                  "9oter": "Other Territories"}

    # Format the sorted data and print it
    print("\nGreater Capital City Number of Tweets Made")
    for city_code, count in gather_city.items():
        if city_code in city_names:
            city_name = city_names[city_code]
            print(f"{city_code} ({city_name})    {count:,}")

    # Task3
    def sort_key(item):
        # Extract the first element of the tuple and return its negative value
        return -item[0]


    city_codes = ["1gsyd", "2gmel", "5gper",
                  "3gbri", "4gade", "8acte",
                  "6ghob", "7gdar", "9oter"]

    sorted_data = dict(sorted(author_city.items(),
                              key=lambda x: ((9 - x[1].count(0)), sum(x[1])),
                              reverse=True)[:10])

    print("\nRank\tAuthor Id\tNumber of Unique City Locations and #Tweets")
    rank = 1
    zero_count = []
    total_tweet = []
    for author, stats in sorted_data.items():
        zero_count.append(stats.count(0))
        total_tweet.append(sum(stats))
        for i in range(len(stats)):
            stats[i] = (stats[i], i)

        sorted_data[author] = sorted(stats, key=sort_key)

    for author, stats in sorted_data.items():

        for i in range(len(stats)):
            stats[i] = str(stats[i][0]) + city_codes[i][1::]

        print(f" {rank}      {author}     {9 - zero_count[rank - 1]}  ({total_tweet[rank - 1]}tweets  {stats} )")
        rank += 1
