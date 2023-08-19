from datetime import datetime
import logging


logging.basicConfig(
    level=logging.INFO,
    filename=f'pipline_out_{datetime.now().strftime("%d-%m-%Y-%H:%M:%S")}.log',
    filemode="a",
)

# logging.basicConfig(
#     filename="log_file_test.log",

#     format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
#     datefmt="%H:%M:%S",
#     level=logging.DEBUG,
# )


main_logger = logging.getLogger()
