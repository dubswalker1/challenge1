import asyncio
import time
import numpy
from sys import byteorder as sys_byteorder

async def connect(host, port):
    # connect to host and port, with retries
    RETRY_TIME = 5
    MAX_RETRIES = 10
    retries = 0
    reader, writer = None, None
    print("Trying to connect {}:{}".format(host, port))
    while True and retries < MAX_RETRIES:
        try:
            reader, writer = await asyncio.open_connection(
                                host, port)
            break
        except Exception as e:
            print("Failed to connect {} {}: {}".format(host, port, e))
            print("  remaining retries: "+str(MAX_RETRIES - retries))
            retries+=1
            await asyncio.sleep(
                RETRY_TIME
            )
    if reader == None:
        return None, None
    print("Connected to {}:{}".format(host, port))
    return reader, writer

async def receive_data(host="127.0.0.1", port=8888, max_time=600):
    # connect to the sender server to start receiving data
    start = time.time()
    loop = asyncio.get_running_loop()
    DATA_INC = 128 # in bytes 128 bytes in 1024, each int16 is 2 bytes
    SLEEP_TIME = 1 # time to sleep to wait for more data

    # Register the open socket to wait for data from the "sender" server
    reader, writer = await connect(host=host, port=port)

    # exit if no connection made
    if reader == None:
        return 0

    # Wait for data ... just keep going until max time reached
    total_kb = 0
    while (time.time() - start) < max_time:
        # read a kb at a time
        data = await reader.read(DATA_INC) # read a kb at a time
        if not data:
            await asyncio.sleep(SLEEP_TIME)# sleep 1 second to wait for more data
            continue

        # build up the array of 16bit int values from the incoming data
        list_16bits = [int.from_bytes(data[i:i+2], byteorder=sys_byteorder) \
                            for i in range(0,len(data),2)]
        # decode all values in the list into numpy int16
        int16_array = numpy.array(list_16bits, dtype=numpy.int16)
        
        print("Received kb of int16: "+str(int16_array))
        # not consuming the data for anything, just summing up what we've received
        total_kb+=len(data)/DATA_INC
        print("Running Total Rcvd kb: "+str(total_kb))
    print("Total: "+str(total_kb))
    # we are done, close the connection
    writer.close()
    return total_kb

async def handle_rcv_serving(reader, writer):
    # receiver doesn't do much as a server, just accept messages and respond
    data = await reader.read(100)
    message = data.decode()
    addr = writer.get_extra_info('peername')
    reply = "hello, I am the receiving server, I just receive data when I start up, "\
                "if the sender server is running."
    writer.write(reply.encode())

    print(f"Received {message!r} from {addr!r}")

async def main():
    server = await asyncio.start_server(
        handle_rcv_serving, '127.0.0.1', 8880)

    MAX_TIME = 600 # 10 mins to seconds
    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f'Serving on {addrs}')

    # create a receive task to get the data from the sender server
    receive_task = asyncio.create_task(receive_data(
                            host='127.0.0.1', 
                            port=8888,
                            max_time=MAX_TIME))
    await receive_task

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())