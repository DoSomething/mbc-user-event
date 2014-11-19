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
require __DIR__ . '/mb-secure-config.inc';
require __DIR__ . '/mb-config.inc';

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
        ),
        'global_merge_vars' => array(
          'name' => 'MEMBER_COUNT',
          'content' => $this->getMemberCount()
        ),
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

    echo '------- MBC_UserEvent_Birthday->sendBirthdayEmails START - ' . date('D M j G:i:s T Y') . ' -------', "\n";

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
        ),
      );
      $delivery_tags[] = $recipient['delivery_tag'];
    }

    $templateName = 'mb-userevent-birthday-v2';
    $templateContent = array();
    $message = array(
      'from_email' => 'no-reply@dosomething.org',
      'from_name' => 'DoSomething.org',
      'subject' => 'Happy Birthday from DoSomething.org',
      'to' => $to,
      'merge_vars' => $merge_vars,
      'tags' => array('user-event', 'birthday'),
      '' => ''
    );

    // Use the Mandrill service
    $mandrill = new Mandrill();
    $mandrillResults = $mandrill->messages->sendTemplate($templateName, $templateContent, $message);

    $statHat = new StatHat($this->settings['stathat_ez_key'], 'mbc-user-event_birthday:');
    $statHat->setIsProduction(TRUE);

    // ack messages to remove them from the queue, trap errors
    foreach($mandrillResults as $resultCount => $resultDetails) {
      if ($resultDetails['status'] == 'invalid') {
        echo '******* MBC_UserEvent_Birthday->sendBirthdayEmails Mandrill ERROR: "invalid" -> ' . $resultDetails['email'] . ' as Send-Template submission - ' . date('D M j G:i:s T Y') . ' *******', "\n";
        $statHat->addStatName('sendBirthdayEmails_MandrillERROR_invalid');
      }
      elseif (!$resultDetails['status'] == 'sent' && !$resultDetails['status'] == 'queued') {
        echo '******* MBC_UserEvent_Birthday->sendBirthdayEmails Mandrill ERROR: "Unknown" -> ' . print_r($resultDetails, TRUE) . ' as Send-Template submission - ' . date('D M j G:i:s T Y') . ' *******', "\n";
        $statHat->addStatName('sendBirthdayEmails_MandrillERROR_unknown');
      }
      else {
        $statHat->addStatName('sendBirthdayEmails_MandrillSent');
      }
      $statHat->reportCount(1);
      $statHat->clearAddedStatNames();

      $this->channel->basic_ack($delivery_tags[$resultCount]);
    }

    echo '------- MBC_UserEvent_Birthday->sendBirthdayEmails END: ' . (count($this->recipients) - 1) . ' messages sent as Mandrill Send-Template submission - ' . date('D M j G:i:s T Y') . ' -------', "\n";

  }

  /**
   * Gather current member count via Drupal end point.
   * https://github.com/DoSomething/dosomething/wiki/API#get-member-count
   *
   *  POST https://beta.dosomething.org/api/v1/users/get_member_count
   *
   * @return string $memberCountFormatted
   *   The string supplied byt the Drupal endpoint /get_member_count.
   */
  private function getMemberCount() {

    $curlUrl = getenv('DS_DRUPAL_API_HOST');
    $port = getenv('DS_DRUPAL_API_PORT');
    if ($port != 0) {
      $curlUrl .= ':' . $port;
    }
    $curlUrl .= '/api/v1/users/get_member_count';

    // $post value sent in cURL call intentionally empty due to the endpoint
    // expecting POST rather than GET where there's no POST values expected.
    $post = array();

    $result = $this->curlPOST($curlUrl, $post);
    if (isset($result->readable)) {
      $memberCountFormatted = $result->readable;
    }
    else {
      $memberCountFormatted = NULL;
    }
    return $memberCountFormatted;

  }

  /**
   * cURL POSTs
   *
   * @return object $result
   *   The results retruned from the cURL call.
   */
  private function curlPOST($curlUrl, $post) {

    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, $curlUrl);
    curl_setopt($ch, CURLOPT_POST, 1);
    curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($post));
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
    curl_setopt($ch, CURLOPT_HTTPHEADER,
      array(
        'Content-type: application/json',
        'Accept: application/json'
      )
    );
    curl_setopt($ch,CURLOPT_CONNECTTIMEOUT, 3);
    curl_setopt($ch,CURLOPT_TIMEOUT, 20);
    $jsonResult = curl_exec($ch);
    $result = json_decode($jsonResult);
    curl_close($ch);

    return $result;
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
);

echo '------- mbc-user-event_birthday START: ' . date('D M j G:i:s T Y') . ' -------', "\n";

// Kick Off
$ub = new MBC_UserEvent_Birthday($credentials, $config, $settings);
$ub->consumeBirthdayQueue();

echo '------- mbp-user-event_birthday END: ' . date('D M j G:i:s T Y') . ' -------', "\n";