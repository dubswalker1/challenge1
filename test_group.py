import pytest
import asyncio
from sender import generate_data
from receiver import receive_data

@pytest.fixture(scope="session")
def event_loop():
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()

@pytest.mark.asyncio
class TestGroup:
    async def test_generate_data(self):
        # test the data/file generation functionality
        file_queue = asyncio.Queue()
        total_files = 10 # just for testing
        total_generated, return_sizes, return_intervals, total_files = await generate_data(
                        duration_seconds=600, \
                        max_files=total_files, \
                        min_size_kb=1, \
                        max_size_kb=1000,\
                        min_interval_ms=1, \
                        max_interval_ms=1000,
                        file_queue=file_queue)

        # test that the generation parameters are met
        for size in return_sizes:
            assert(size >= 1 and size <= 1000)
        for interval in return_intervals:
            assert(interval >= 1 and interval <= 1000)
        assert(total_generated > 0)
        assert(total_files == total_files)

    async def test_receive_data(self):
        # test the sending/receiving data functionality
        # this requires a sender server to be up and running before running the test
        # --> python sender.py
        # for the test, not using the full 600 seconds of time, 
        # just 10 seconds to see that it works
        received_kb = await receive_data(host="127.0.0.1", port=8888, max_time=10)

        assert(received_kb > 0)

    async def test_receiver_server(self):
        # test that the receiver is working
        # Register the open socket to wait for data.
        reader, writer = await asyncio.open_connection(host="127.0.0.1", port=8880)

        # just send something ... anything
        writer.write("hello".encode())

        # Wait for data
        data = await reader.read(150)
        assert(data != None)
        
        # Got data, we are done: close the socket
        print("Received:", data.decode())
        writer.close()
        





        