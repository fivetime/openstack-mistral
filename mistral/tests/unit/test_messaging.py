# Copyright 2026 - Fivetime, Ltd.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

from unittest import mock

from oslo_config import cfg

from mistral import messaging
from mistral.tests.unit import base

CONF = cfg.CONF


class StartListenerTest(base.BaseTest):
    """Tests for mistral.messaging.start_listener()."""

    @mock.patch('mistral.messaging.listener.get_notification_listener')
    @mock.patch('oslo_messaging.get_transport')
    @mock.patch('oslo_messaging.get_notification_transport')
    def test_listener_is_built_on_the_notification_transport(
            self, get_notification_transport, get_transport, get_listener):
        # Event triggers are driven by notifications, and notifications can
        # be routed to a bus of their own through
        # [oslo_messaging_notifications] transport_url. A listener built on
        # the RPC transport from [DEFAULT] subscribes where no event is ever
        # published, so the triggers never fire and nothing says why.
        result = messaging.start_listener(
            CONF, 'cinder', 'versioned_notifications', []
        )

        get_notification_transport.assert_called_once_with(CONF)
        get_transport.assert_not_called()

        self.assertEqual(
            get_notification_transport.return_value,
            get_listener.call_args[0][0]
        )

        self.assertEqual(get_listener.return_value, result)
        result.start.assert_called_once_with()

    @mock.patch('mistral.messaging.listener.get_notification_listener')
    @mock.patch('oslo_messaging.get_notification_transport')
    def test_listener_target_and_pool(self, _transport, get_listener):
        messaging.start_listener(
            CONF, 'cinder', 'versioned_notifications', []
        )

        targets = get_listener.call_args[0][1]

        self.assertEqual(1, len(targets))
        self.assertEqual('cinder', targets[0].exchange)
        self.assertEqual('versioned_notifications', targets[0].topic)
        self.assertEqual(
            messaging.get_pool_name('cinder'),
            get_listener.call_args[1]['pool']
        )
