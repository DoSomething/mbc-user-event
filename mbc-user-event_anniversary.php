<?php
/**
 * mbc-user-event.php
 *
 * Collect users for different event types based on a specific date (typically
 * today). Users found are added to the event queues to be consumed by
 * mbc-user-event.
 */

// Load up the Composer autoload magic
require_once __DIR__ . '/vendor/autoload.php';

// Load configuration settings common to the Message Broker system
// symlinks in the project directory point to the actual location of the files
require __DIR__ . '/mb-secure-config.inc';
require __DIR__ . '/mb-config.inc';

class MBC_UserEvent_Anniversary
{

  const BATCH_SIZE = 500;

  /**
   * Message Broker connection to RabbitMQ
   */
  private $messageBroker;

  /**
   * Configuration settings
   */
  private $config;

  /**
   * Configuration settings
   */
  private $channel;
  
  /**
   * A list of recipients to send messages to
   */
  private $recipients;

  /**
   * Setting from external services - Mailchimp.
   *
   * @var array
   */
  private $statHat;

  /**
   * Constructor - setup parameters to be accessed by class methods
   *
   * @param array $messageCount
   *   The number of messages currently in the queue wait to be consumed.
   *
   * @param object $messageBroker
   *   The connection object to the RabbitMQ server.
   */
  public function __construct($credentials, $config) {
    $this->messageBroker = new MessageBroker($credentials, $config);
    $this->config = $config;
    $this->channel = $this->messageBroker->connection->channel();

    // $this->statHat = new StatHat($settings['stathat_ez_key'], 'mbc_user_event_birthday:');
    // $this->statHat->setIsProduction(FALSE);
  }

  /**
   * Consume userBirthday queue to collect data for Mandrill Send-Template
   * submission
   *
   * @param array $payload
   *   The contents of the queue entry
   */
  public function consumeAnniversaryQueue() {
    
    
$bla = FALSE;
if ($bla) {
  $bla = TRUE;
}

    $this->recipients = array();

    // How many messages are waiting to be processed?
    list($this->channel, $status) = $this->messageBroker->setupQueue($this->config['queue'][0]['name'], $this->channel);
    $messageCount = $status[1];
    $processedCount = 0;

    while ($messageCount > 0 && $processedCount <= self::BATCH_SIZE) {
      $messageDetails = $this->channel->basic_get($this->config['queue'][0]['name']);
      $messagePayload = unserialize($messageDetails->body);
      $years = date('Y') - $messagePayload['year'];
      $anniversary = $years . date('S', mktime(1, 1, 1, 1, ( (($years >= 10) + ($years >= 20) + ($years == 0))*10 + $years%10) ));
      $this->recipients[] = array(
        'email' => $messagePayload['email'],
        'uid' => $messagePayload['drupal_uid'],
        'delivery_tag' => $messageDetails->delivery_info['delivery_tag'],
        'merge_vars' => array(
          'FNAME' => $messagePayload['merge_vars']['FNAME'],
          'ANNIVERSARY' => $anniversary,
        )
      );
      $messageCount--;
      $processedCount++;
    }

    $this->sendAnniversaryEmails();
  }

  /**
   * Send user anniversary email
   *
   * @param array $payload
   *   The contents of the queue entry
   */
  private function sendAnniversaryEmails() {
    
    
$bla = FALSE;
if ($bla) {
  $bla = TRUE;
}

    echo '------- MBC_UserEvent_Anniversary->sendAnniversaryEmails START - ' . date('D M j G:i:s T Y') . ' -------', "\n";

    $to = array();
    $merge_vars = array();

    // Build out $message to send to Mandrill
    foreach ($this->recipients as $recipient) {
      $to[] = array(
        'email' => $recipient['email'],
        'name' => $recipient['merge_vars']['FNAME'],
      );
      $merge_vars[] = array(
        'rcpt' => $recipient['email'],
        'vars' => array(
          0 => array(
            'name' => 'FNAME',
            'content' => $recipient['merge_vars']['FNAME'],
          ),
          1 => array(
            'name' => 'ANNIVERSARY',
            'content' => $recipient['merge_vars']['ANNIVERSARY'],
          ),
          2 => array(
            'name' => 'UID',
            'content' => $recipient['uid'],
          ),
        ),
      );
      $delivery_tags[] = $recipient['delivery_tag'];
    }
    
    $to = array();
    $to[0] = array(
      'email' => 'dlee@dosomething.org',
      'name' => 'Darren',
    );
    $to[1] = array(
      'email' => 'mlidey@dosomething.org',
      'name' => 'Marah',
    );
    $to[2] = array(
      'email' => 'cbell@dosomething.org',
      'name' => 'Crystal',
    );
    $to[3] = array(
      'email' => 'juy@dosomething.org',
      'name' => 'Jonathan',
    );
    $merge_vars = array();
    $merge_vars[0] = array(
      'rcpt' => 'dlee@dosomething.org',
      'vars' => array(
        0 => array(
          'name' => 'FNAME',
          'content' => 'Darren',
        ),
        1 => array(
          'name' => 'ANNIVERSARY',
          'content' => '2nd',
        ),
        2 => array(
          'name' => 'UID',
          'content' => '123456',
        )
      ),
    );
    $merge_vars[1] = array(
      'rcpt' => 'mlidey@dosomething.org',
      'vars' => array(
        0 => array(
          'name' => 'FNAME',
          'content' => 'Marah',
        ),
        1 => array(
          'name' => 'ANNIVERSARY',
          'content' => '3rd',
        ),
        2 => array(
          'name' => 'UID',
          'content' => '234567',
        )
      ),
    );
    $merge_vars[2] = array(
      'rcpt' => 'cbell@dosomething.org',
      'vars' => array(
        0 => array(
          'name' => 'FNAME',
          'content' => 'Crystal',
        ),
        1 => array(
          'name' => 'ANNIVERSARY',
          'content' => '1st',
        ),
        2 => array(
          'name' => 'UID',
          'content' => '345678',
        )
      ),
    );
    $merge_vars[3] = array(
      'rcpt' => 'juy@dosomething.org',
      'vars' => array(
        0 => array(
          'name' => 'FNAME',
          'content' => 'Jonathan',
        ),
        1 => array(
          'name' => 'ANNIVERSARY',
          'content' => '5th',
        ),
        2 => array(
          'name' => 'UID',
          'content' => '345678',
        )
      ),
    );
    
    
$bla = FALSE;
if ($bla) {
  $bla = TRUE;
}

    $templateName = 'mb-user-anniversary';
    $templateContent = array();
    $message = array(
      'from_email' => 'no-reply@dosomething.org',
      'from_name' => 'DoSomething.org',
      'subject' => 'Happy Anniversary from DoSomething.org',
      'to' => $to,
      'merge_vars' => $merge_vars,
      'tags' => array('user-event', 'anniversary'),
    );

    // Use the Mandrill service
    $mandrill = new Mandrill();

    // Send message
    $mandrillResults = $mandrill->messages->sendTemplate($templateName, $templateContent, $message);

    // ack messages to remove them from the queue, trap errors
    foreach($mandrillResults as $resultCount => $resultDetails) {
      if ($resultDetails['status'] == 'invalid') {
        echo '******* MBC_UserEvent_Anniversary->sendAnniversaryEmails Mandrill ERROR: "invalid" -> ' . $resultDetails['email'] . ' as Send-Template submission - ' . date('D M j G:i:s T Y') . ' *******', "\n";
      }
      elseif (!$resultDetails['status'] == 'sent') {
        echo '******* MBC_UserEvent_Anniversary->sendAnniversaryEmails Mandrill ERROR: "Unknown" -> ' . print_r($resultDetails, TRUE) . ' as Send-Template submission - ' . date('D M j G:i:s T Y') . ' *******', "\n";
      }
      $this->channel->basic_ack($delivery_tags[$resultCount]);
    }

    echo '------- MBC_UserEvent_Anniversary->sendAnniversaryEmails END: ' . count($this->recipients) . ' messages sent as Mandrill Send-Template submission - ' . date('D M j G:i:s T Y') . ' -------', "\n";

  }

}

// Settings
$credentials = array(
  'host' =>  getenv("RABBITMQ_HOST"),
  'port' => getenv("RABBITMQ_PORT"),
  'username' => getenv("RABBITMQ_USERNAME"),
  'password' => getenv("RABBITMQ_PASSWORD"),
  'vhost' => getenv("RABBITMQ_VHOST"),
);

$config = array(
  'exchange' => array(
    'name' => getenv("MB_USER_EVENT_EXCHANGE"),
    'type' => getenv("MB_USER_EVENT_EXCHANGE_TYPE"),
    'passive' => getenv("MB_USER_EVENT_EXCHANGE_PASSIVE"),
    'durable' => getenv("MB_USER_EVENT_EXCHANGE_DURABLE"),
    'auto_delete' => getenv("MB_USER_EVENT_EXCHANGE_AUTO_DELETE"),
  ),
  'queue' => array(
    array(
      'name' => getenv("MB_USER_EVENT_ANNIVERSARY_QUEUE"),
      'passive' => getenv("MB_USER_EVENT_ANNIVERSARY_QUEUE_PASSIVE"),
      'durable' => getenv("MB_USER_EVENT_ANNIVERSARY_QUEUE_DURABLE"),
      'exclusive' => getenv("MB_USER_EVENT_ANNIVERSARYP_QUEUE_EXCLUSIVE"),
      'auto_delete' => getenv("MB_USER_EVENT_ANNIVERSARY_QUEUE_AUTO_DELETE"),
      'bindingKey' => getenv("MB_USER_EVENT_ANNIVERSARY_QUEUE_BINDING_KEY"),
    ),
  ),
  'routingKey' => getenv("MB_USER_EVENT_ANNIVERSARY_ROUTING_KEY"),
);

echo '------- mbc-user-event_anniversary START: ' . date('D M j G:i:s T Y') . ' -------', "\n";

// Kick Off
$ub = new MBC_UserEvent_Anniversary($credentials, $config);
$ub->consumeAnniversaryQueue();

echo '------- mbp-user-event_anniversary END: ' . date('D M j G:i:s T Y') . ' -------', "\n";