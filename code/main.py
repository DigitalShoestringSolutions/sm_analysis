from functools import lru_cache
import logging
import traceback
import importlib
import asyncio
import argparse
import uvicorn

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

import config_manager

logger = logging.getLogger("main")


@lru_cache
def get_settings(module_file, user_file):
    return config_manager.get_config(module_file, user_file)


async def catch_request(request):
    data, status = await handle_catch(request, request.path_params['full_path'], request.method, request.app.state.settings)
    return JSONResponse(data, status_code=status)


async def handle_catch(
    request,
    path,
    method,
    settings
):

    logger.debug(f"Handling {method} on {path}")
    clean_path = path.strip('/').lstrip('/')

    for route_name, route_entry in settings['routes'].items():
        if route_entry["path"].strip('/').lstrip('/') == clean_path and method.lower() in (m.lower() for m in route_entry["methods"]):
            return await call_route(route_name, route_entry, request.query_params, settings['config'])

    return {"type": "error", "message": "no route defined", "details": {"method": method, "path": path}}, 404


async def call_route(name, entry, query_params, global_config):
    module_name = entry["module"]
    function_name = entry["function"]
    full_module_name = f"analysis_modules.{module_name}"
    entry_config = entry.get("config", {})

    try:
        module = importlib.import_module(full_module_name)
        logger.debug(f"Imported {module_name}")
        function = getattr(module, function_name, None)
        if function:
            function_response = function(
                {"params": query_params, "config": entry_config, "global_config": global_config})

            if asyncio.iscoroutine(function_response):
                result = await function_response
            else:
                result = function_response

            return result, 200
        else:
            return {"type": "error", "message": "couldn't find function"}, 500
    except ModuleNotFoundError:
        logger.error(
            f"Unable to import module {module_name}. Service Module may not function as intended")
        logger.error(traceback.format_exc())
        return {"type": "error", "message": "unable to import module"}, 500
    except:
        logger.error(traceback.format_exc())
        return {"type": "error", "message": "Something went wrong"}, 500


def handle_args():
    levels = {'debug': logging.DEBUG, 'info': logging.INFO,
              'warning': logging.WARNING, 'error': logging.ERROR}
    parser = argparse.ArgumentParser(description='Analysis service module.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--log", choices=['debug', 'info', 'warning', 'error'], help="Log level", default='info',
                        type=str)
    parser.add_argument("--module_config", help="Module config file", type=str)
    parser.add_argument("--user_config", help="User config file", type=str)
    args = parser.parse_args()

    log_level = levels.get(args.log, logging.INFO)
    module_config_file = args.module_config
    user_config_file = args.user_config

    return {"log_level": log_level, "module_config_file": module_config_file, "user_config_file": user_config_file}


if __name__ == "__main__":
    args = handle_args()
    logging.basicConfig(level=args['log_level'])

    routes = [
        Route("/{full_path:path}", endpoint=catch_request,
              methods=['GET', 'POST']),
    ]

    middleware = [
        Middleware(CORSMiddleware, allow_origins=['*'], allow_methods=['*'],allow_headers=['*'])
    ]

    app = Starlette(routes=routes, middleware=middleware)
    app.state.settings = get_settings(
        args.get('module_config_file'), args.get('user_config_file'))

    uvicorn.run(app, host="0.0.0.0", port=80, log_level="info")
