import aiohttp, aiohttp.web
import logging
import asyncio
import aiohttp_cors

logger = logging.getLogger(__name__)


async def get_all_request_data(request):
    url_vars = {k: v for k, v in request.match_info.items()}
    query_params = {k: request.query.getall(k) for k in request.query.keys()}
    body_data = await request.json() if request.has_body else {}

    logger.debug(
        f"{request.url} | url:{url_vars} | query:{query_params} | body:{body_data}"
    )
    return {"url": url_vars, "query": query_params, "body": body_data}


class HTTPTrigger:
    def __init__(self, scheduler, config):
        self.app = aiohttp.web.Application()
        self.scheduler = scheduler
        self.config = config

    def register_response(self, method, path, func):
        async def request_response_wrapper(request):
            request_data = await get_all_request_data(request)

            # call task
            result_code, result_body = await func(request_data, config=self.config)

            response = aiohttp.web.json_response(result_body)
            response.set_status(result_code)
            return response

        self.app.router.add_route(method, path, request_response_wrapper)

    # this is a decorator
    def response(self, method, path):
        def inner(func):
            self.register_response(method, path, func)
            return func

        return inner

    def register_dispatch(self, method, path, func):
        async def dispatch_task_wrapper(request):
            request_data = await get_all_request_data(request)

            schedule_success = self.scheduler.schedule_soon(
                func, [request_data], {"config": self.config}
            )

            if schedule_success:
                return aiohttp.web.json_response(
                    {"status": "task submitted", "task": func.__name__}, status=200
                )
            else:
                return aiohttp.web.json_response(status=500)

        self.app.router.add_route(method, path, dispatch_task_wrapper)

    # this is a decorator
    def dispatch(self, method, path):
        def inner(func):
            self.register_dispatch(method, path, func)
            return func

        return inner

    def should_run(self):
        return len(self.app.router._resources) > 0

    # This is the coroutine that runs the server
    async def run(self):
        cors = aiohttp_cors.setup(
            self.app,
            defaults={
                "*": aiohttp_cors.ResourceOptions(
                    allow_credentials=True,
                    expose_headers="*",
                    allow_headers="*",
                    allow_methods="*",
                )
            },
        )

        for route in list(self.app.router.resources()): # list() to iterate over a copy as it changes
            if isinstance(route, aiohttp.web.Resource): # Filter for actual URL resources
                cors.add(route) # This applies the defaults specified above
        logging.getLogger("aiohttp").setLevel(logging.INFO)
        runner = aiohttp.web.AppRunner(self.app)
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, port=80)
        await site.start()

        try:
            while True:
                await asyncio.sleep(3600)
        finally:
            # wait for finish signal
            await runner.cleanup()
