#include <stdio.h>
#include <nanomsg/nn.h>
#include <nanomsg/pubsub.h>
#include <assert.h>
#include <errno.h>
#include <string.h>

#include "control.h"
#include "globals.h"

#define NN_IN 1
#define NN_OUT 2

const char *vertex_control_channel = "+vertex_control";

int mode = MODE_RECEIVING;

int main (int argc, char **argv)
{
    struct nn_pollfd pfd[2];
    int rc;
    void *buf = NULL;

    printf("Starting vertex broker\n" );

    int pub = nn_socket (AF_SP, NN_PUB);
    assert (pub >= 0);
    assert (nn_bind (pub, "tcp://0.0.0.0:8690") >= 0);

    int sub = nn_socket (AF_SP, NN_SUB);
    assert (sub >= 0);

    assert (nn_setsockopt (sub, NN_SUB, NN_SUB_SUBSCRIBE, "", 0) >= 0);
    assert (nn_bind (sub, "tcp://0.0.0.0:8689") >= 0);

    int controlsub = nn_socket (AF_SP, NN_SUB);
    assert (controlsub >= 0);
    assert (nn_setsockopt (controlsub, NN_SUB, NN_SUB_SUBSCRIBE, vertex_control_channel, strlen(vertex_control_channel)) >= 0);
    assert (nn_connect (controlsub, "tcp://localhost:8687") >= 0);

    printf("Ready for polling\n" );

    /*  Initialise the pollset. */
    pfd[0].fd = pub;
    // only set this poll event if we actually have something to send to prevent infinite loop
    pfd[0].events = 0;//NN_POLLOUT;
    pfd[1].fd = sub;
    pfd[1].events = NN_POLLIN;
    //pfd[2].fd = controlsub;
    //pfd[2].events = NN_POLLIN;

    while (1) {
        rc = nn_poll (pfd, 2, 2000);
        if (rc == 0) {
            // timeout. Check if we need to continue
            continue;
        }
        if (rc == -1) {
            // error. Probably break out and shut down
            fprintf( stderr, "nn_poll() error: %s\n", strerror(errno));
            break;
        }
        if (pfd [0].revents & NN_POLLOUT) {
            // vertex PUB is ready to send another message
            printf("PUB message ready\n" );
        }
        if (pfd [1].revents & NN_POLLIN) {
            // vertex SUB is receiving a message
            rc = nn_recv (sub, &buf, NN_MSG, NN_DONTWAIT);
            if ( rc < 0 ) {
                if ( rc == EAGAIN ) {
                    printf( "Poll indicated readiness to read, but returned EAGAIN\n" );
                } 
                fprintf( stderr, "nn_poll() error: %s\n", strerror(errno));
                break;
            } else {
                // do something with buf and rc (len)
                char tst[100] = {"\0"};
                snprintf( tst, 100, "%s", buf );
                printf( "%s\n", tst );

                // sheer violation of POLLOUT, but who cares..?
                nn_send(pub, &buf, NN_MSG, NN_DONTWAIT );

                // no need to deallocate. We just did zero copy.
                // nn_freemsg (buf);
            }
        }
        /*
        if (pfd [2].revents & NN_POLLIN) {
            // control SUB is receiving a message
            printf("control SUB message ready\n" );
            rc = nn_recv (controlsub, buf, NN_MSG, NN_DONTWAIT);
            if ( rc < 0 ) {
                if ( rc == EAGAIN ) {
                    printf( "Poll indicated readiness to read, but returned EAGAIN\n" );
                } 
                fprintf( stderr, "nn_poll() error: %s\n", strerror(errno));
                break;
            } else {
                // do something with buf and rc (len)
                nn_freemsg (buf);
            }
        }
        */
    }

    nn_shutdown (controlsub, 0);
    nn_shutdown (sub, 0);
    nn_shutdown (pub, 0);

    return 0;
}

