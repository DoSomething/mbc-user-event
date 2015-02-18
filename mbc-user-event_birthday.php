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
use DoSomething\MBStatTracker\StatHat;

// Load configuration settings common to the Message Broker system
// symlinks in the project directory point to the actual location of the files
require_once __DIR__ . '/mb-secure-config.inc';
require_once __DIR__ . '/mb-config.inc';

class MBC_UserEvent_Birthday
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
   * Settings
   */
  private $settings;

  /**
   * Configuration settings
   */
  private $channel;

  /**
   * Collection of helper methods
   *
   * @var object
   */
  private $toolbox;

  /**
   * The MEMBER_COUNT value from the DoSomething.org API via MB_Toolbox
   *
   * @var string
   */
  private $memberCount;

  /**
   * A list of recipients to send messages to
   */
  private $recipients;

  /**
   * Constructor - setup parameters to be accessed by class methods
   *
   * @param array $messageCount
   *   The number of messages currently in the queue wait to be consumed.
   *
   * @param object $messageBroker
   *   The connection object to the RabbitMQ server.
   */
  public function __construct($credentials, $config, $settings) {
    $this->messageBroker = new MessageBroker($credentials, $config);
    $this->channel = $this->messageBroker->connection->channel();
    $this->config = $config;
    $this->settings = $settings;

    $this->toolbox = new MB_Toolbox($settings);
    $this->memberCount = $this->toolbox->getDSMemberCount();
  }

  /**
   * Consume userBirthday queue to collect data for Mandrill Send-Template
   * submission
   *
   * @param array $payload
   *   The contents of the queue entry
   */
  public function consumeBirthdayQueue() {

    $this->recipients = array();

    // How many messages are waiting to be processed?
    list($this->channel, $status) = $this->messageBroker->setupQueue($this->config['queue'][0]['name'], $this->channel);
    $messageCount = $status[1];
    $processedCount = 0;

    while ($messageCount > 0 && $processedCount <= self::BATCH_SIZE) {
      $messageDetails = $this->channel->basic_get($this->config['queue'][0]['name']);
      $messagePayload = unserialize($messageDetails->body);
      $this->recipients[] = array(
        'email' => $messagePayload['email'],
        'delivery_tag' => $messageDetails->delivery_info['delivery_tag'],
        'merge_vars' => array(
          'FNAME' => $messagePayload['merge_vars']['FNAME'],
          'SUBSRIPTIONS_LINK' => $toolbox->subscriptionsLinkGenerator($messagePayload['email']),
        )
      );
      $messageCount--;
      $processedCount++;
    }

    $statHat = new StatHat($this->settings['stathat_ez_key'], 'mbc-user-event_birthday:');
    $statHat->setIsProduction(TRUE);
    $statHat->addStatName('consumeBirthdayQueue');
    $statHat->reportCount($processedCount);

    $this->sendBirthdayEmails();

  }

  /**
   * Send user birthday email
   *
   * @param array $payload
   *   The contents of the queue entry
   */
  private function sendBirthdayEmails() {

    echo '------- MBC_UserEvent_Birthday->sendBirthdayEmails START - ' . date('D M j G:i:s T Y') . ' -------', PHP_EOL;

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
            'name' => 'SUBSRIPTIONS_LINK',
            'content' => $recipient['merge_vars']['SUBSRIPTIONS_LINK'],
          ),
        ),
      );
      $delivery_tags[] = $recipient['delivery_tag'];
    }

    $globalMergeVars = array(
      0 => array(
        'name' => 'MEMBER_COUNT',
        'content' => $this->memberCount,
      ),
    );

    $templateName = 'mb-userevent-birthday-v2';
    $templateContent = array();
    $message = array(
      'from_email' => 'no-reply@dosomething.org',
      'from_name' => 'DoSomething.org',
      'subject' => 'Happy Birthday from DoSomething.org',
      'to' => $to,
      'merge_vars' => $merge_vars,
      'global_merge_vars' => $globalMergeVars,
      'tags' => array('user-event', 'birthday'),
    );

    // Use the Mandrill service
    $mandrill = new Mandrill();
    $mandrillResults = $mandrill->messages->sendTemplate($templateName, $templateContent, $message);

    $statHat = new StatHat($this->settings['stathat_ez_key'], 'mbc-user-event_birthday:');
    $statHat->setIsProduction(TRUE);

    // ack messages to remove them from the queue, trap errors
    foreach($mandrillResults as $resultCount => $resultDetails) {
      if ($resultDetails['status'] == 'invalid') {
        echo '******* MBC_UserEvent_Birthday->sendBirthdayEmails Mandrill ERROR: "invalid" -> ' . $resultDetails['email'] . ' as Send-Template submission - ' . date('D M j G:i:s T Y') . ' *******', PHP_EOL;
        $statHat->addStatName('sendBirthdayEmails_MandrillERROR_invalid');
      }
      elseif (!$resultDetails['status'] == 'sent' && !$resultDetails['status'] == 'queued') {
        echo '******* MBC_UserEvent_Birthday->sendBirthdayEmails Mandrill ERROR: "Unknown" -> ' . print_r($resultDetails, TRUE) . ' as Send-Template submission - ' . date('D M j G:i:s T Y') . ' *******', PHP_EOL;
        $statHat->addStatName('sendBirthdayEmails_MandrillERROR_unknown');
      }
      else {
        $statHat->addStatName('sendBirthdayEmails_MandrillSent');
      }
      $statHat->reportCount(1);
      $statHat->clearAddedStatNames();

      $this->channel->basic_ack($delivery_tags[$resultCount]);
    }

    echo '------- MBC_UserEvent_Birthday->sendBirthdayEmails END: ' . (count($this->recipients) - 1) . ' messages sent as Mandrill Send-Template submission - ' . date('D M j G:i:s T Y') . ' -------', PHP_EOL;

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
      'name' => getenv("MB_USER_EVENT_BIRTHDAY_QUEUE"),
      'passive' => getenv("MB_USER_EVENT_BIRTHDAY_QUEUE_PASSIVE"),
      'durable' => getenv("MB_USER_EVENT_BIRTHDAY_QUEUE_DURABLE"),
      'exclusive' => getenv("MB_USER_EVENT_BIRTHDAY_QUEUE_EXCLUSIVE"),
      'auto_delete' => getenv("MB_USER_EVENT_BIRTHDAY_QUEUE_AUTO_DELETE"),
      'bindingKey' => getenv("MB_USER_EVENT_BIRTHDAY_QUEUE_BINDING_KEY"),
    ),
  ),
  'routingKey' => getenv("MB_USER_EVENT_BIRTHDAY_ROUTING_KEY"),
);
$settings = array(
  'stathat_ez_key' => getenv("STATHAT_EZKEY"),
  'ds_drupal_api_host' => getenv('DS_DRUPAL_API_HOST'),
  'ds_drupal_api_port' => getenv('DS_DRUPAL_API_PORT'),
);

echo '------- mbc-user-event_birthday START: ' . date('D M j G:i:s T Y') . ' -------', PHP_EOL;

// Kick Off
$ub = new MBC_UserEvent_Birthday($credentials, $config, $settings);
$ub->consumeBirthdayQueue();

echo '------- mbp-user-event_birthday END: ' . date('D M j G:i:s T Y') . ' -------', PHP_EOL;