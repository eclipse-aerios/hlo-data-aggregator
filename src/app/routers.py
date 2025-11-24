'''
  Class docstring
'''
import threading
from fastapi import APIRouter, BackgroundTasks
from app.loop import run
from app.utils.log import get_app_logger
from app.app_models.ie_alert_ep_models import IEAlert
from app.utils import ngsild

logger = get_app_logger()


async def kafka_loop():
    '''
    New thread for the kafka consumer
    '''
    thread = threading.Thread(target=run, args=())
    thread.daemon = True  # Optional: makes the thread terminate when the main process does
    thread.start()


router = APIRouter(on_startup=[kafka_loop])


@router.post("/ie-alert",
             responses={
                 200: {
                     "description": "service component alert handling started"
                 },
                 422: {
                     "description": "invalid input"
                 },
             })
async def ie_alert(background_tasks: BackgroundTasks, alert: IEAlert):
    '''
    Reecieve IE rosource alert
    :param alert: Object with triggering events details.
    :return: Response message and status code.
    '''
    logger.info('ServiceComponent id: %s', alert.serviceComponentId)
    logger.info('Triger Event: %s', alert.event)

    # Get pydantic modeled output for allocator
    background_tasks.add_task(ngsild.create_scomponent_for_reorchestration,
                              alert.serviceComponentId)
    
    # All this commented out as moved to background task
    # As it is a long process and all status update goes through Orion with s.component status
    # try:
    #     hlo_allocator_output_py = ngsild.create_scomponent_for_reorchestration(
    #         alert.serviceComponentId)
    #     # logger.info(hlo_allocator_output_py)
    # except Exception as ex:
    #     logger.error(traceback.format_exc())
    #     raise HTTPException(status_code=500) from ex

    # if not hlo_allocator_output_py:
    #     raise HTTPException(
    #         status_code=404,
    #         detail=
    #         f"Sorry to say but Service Component {alert.serviceComponentId} will not leave IE"
    #     )

    # translate to protobuf and send to kafka topic
    # produce_message(hlo_allocator_output_py)

    return {"status": "service component alert handling started, please check service component status for further information"}
