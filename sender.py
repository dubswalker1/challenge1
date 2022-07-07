import asyncio
import time
import pathlib
import numpy
from sys import byteorder as sys_byteorder

async def generate_data(
        duration_seconds=600, \
        max_files=100, \
        min_size_kb=1, \
        max_size_kb=1000,\
        min_interval_ms=1, \
        max_interval_ms=1000,\
        file_queue=None):

    # generate a file of random size, at random intervals, stop at max_files OR duration_seconds
    BINARY_GENERATOR_INT16 = 32767
    # do nothing if no queue passed in
    if file_queue == None:
        return
    
    start = time.time()
    return_intervals = []
    return_sizes = []

    # generate content for the file - just some raw bits that can be added/taken away
    value = numpy.int16(BINARY_GENERATOR_INT16) # choose an arbitrary int16 value as our generator value
    raw_kbit_array = bytearray(value.tobytes()*64) # make this a kb of data 16*64 = 1024

    # initialize a new random number generator
    rng = numpy.random.default_rng() 
    count = 0
    total_generated = 0
    # loop through for each file, generating randomized content out to the file
    while count < max_files and (time.time() - start) < duration_seconds:
        # generate a random time interval at which to generate the next file
        random_interval_ms = rng.integers(low=min_interval_ms, high=max_interval_ms, endpoint=True)
        return_intervals.append(random_interval_ms)

        # pause the time interval before generating the next file
        await asyncio.sleep(random_interval_ms/1000) # convert to seconds

        # generate random kb value (assuming units of 1kb is sufficient fidelity)
        random_length_kb = rng.integers(low=min_size_kb, high=max_size_kb, endpoint=True)
        return_sizes.append(random_length_kb)

        filepath = pathlib.Path.cwd() / 'outputs'
        filepath.mkdir(parents=True, exist_ok=True)
        filepath = filepath / str('file_'+str(count)+'.bits')

        # write out to the file
        with filepath.open('wb') as file:
            # since the content of the files is not important, it is just the length, we use a dummy integer:
            content = raw_kbit_array*random_length_kb # multiply the content (kbit) by the randomized length (in kbit)
            file.write(content)
            print("sent kb: "+str(len(content)/128))
            total_generated+=random_length_kb

        await file_queue.put(filepath)
        count+=1
    print("Total kb Generated: "+str(total_generated))
    return total_generated, return_sizes, return_intervals, count

async def send_files(loop, transport, file_queue):
    start = time.time()
    MAX_TIME = 600
    # run through the queue pulling newly added files
    # continue for MAX_TIME
    while (time.time() - start) < MAX_TIME:
        while file_queue.empty():
            print("waiting for something to send")
            await asyncio.sleep(1)
            # reset start, we've been sleeping this whole time
            start = time.time()

        filepath = await file_queue.get()
        with open(filepath, 'rb') as file:
            # send the file
            print(f"Sending: {filepath!r}")
            await loop.sendfile(transport, file, fallback=True)

async def handle_sending(reader, writer):
    # send data out on the new connection
    # create a file_queue
    file_queue = asyncio.Queue()

    loop = asyncio.get_event_loop()
    transport = writer.transport

    # run the 2 operations concurrently as tasks
    # generate and send as generated
    await asyncio.gather(
        send_files(loop, transport, file_queue),
        generate_data(
                        duration_seconds=600, \
                        max_files=100, \
                        min_size_kb=1, \
                        max_size_kb=1000,\
                        min_interval_ms=1, \
                        max_interval_ms=1000,
                        file_queue=file_queue)
    )

    print("Close the connection")
    writer.close()

async def main():
    # this only performs the callback when a client connects ... 
    server = await asyncio.start_server(
        handle_sending, '127.0.0.1', 8888)

    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f'Serving on {addrs}')

    async with server:
        print("serve forever")
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())