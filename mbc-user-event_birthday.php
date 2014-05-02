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

class MBC_UserEvent_Birthday
{
  
  /**
   * Counter of the number of recipients processed out of the queue
   */
  private $recipientCount;

  /**
   * Batch of birthdays collected from the queue
   */
  private $recipients;

  /**
   * Consume userBirthday queue to collect data for Mandrill Send-Template
   * submission
   *
   * @param array $payload
   *   The contents of the queue entry
   */
  public function consumeBirthdayQueue($payload) {
  
    echo '------- MBC_UserEvent_Birthday->consumeBirthdayQueue START - ' . date('D M j G:i:s:u T Y') . ' -------', "\n";

    $this->recipientCount++;
    $queueIsEmpty =
    
    
    // Get the status details of the queue by requesting a declare
    list($this->channel, $status) = $this->MessageBroker->setupQueue($this->config['queue']['registrations']['name'], $this->channel);

    $messageCount = $status[1];
    // @todo: Respond to unacknowledged messages
    $unackedCount = $status[2];
    
    
    
    $payloadDetails = unserialize($payload->body);
    $this->recipients[] = array(
      'email' => $payloadDetails['email'],
      'FNAME' => $payloadDetails['first_name'],
    );
    
    // If birthday count = 500 or queue is empty, build and send Mandrill submisison
    if ($this->recipientCount >= 500 || $queueIsEmpty) {
      $this->sendBirthdayEmail();
    }
    
    echo '------- MBC_UserEvent_Birthday->consumeBirthdayQueue END  - ' . date('D M j G:i:s:u T Y') . ' -------', "\n";
  }
  
  /**
   * Send user birthday email
   *
   * @param array $payload
   *   The contents of the queue entry
   */
  private function sendBirthdayEmail() {
    
    echo '------- MBC_UserEvent_Birthday->sendBirthdayEmail START - ' . date('D M j G:i:s:u T Y') . ' -------', "\n";
    
    $to = array();
    $merge_vars = array();
    
    // Build out $message to send to Mandrill
    foreach ($this->recipients as $recipient) {
      $to[] = array(
        array(
          'email' => $recipient['email'],
          'name' => $recipient['FNAME'],
        ),
      );
      $merge_vars[] = array(
        array(
          'rcpt' => $recipient['email'],
          'vars' => array(
            'name' => 'FNAME',
            'content' => $recipient['FNAME'],
          ),
        ),
      );
    }
    
    $templateName = '';
    $templateContent = '';
    $message = array(
      'from_email' => 'no-reply@dosomething.org',
      'from_name' => 'DoSomething',
      'subject' => 'Happy Birthday from DoSomething.org',
      'to' => $to,
      'merge_vars' => $merge_vars,
      'tags' => array('user-event', 'birthday'),
    );

    // Use the Mandrill service
    $mandrill = new Mandrill();
    
    // Send message
    $mandrillResults = $mandrill->messages->sendTemplate($templateName, $templateContent, $message);
    
    echo '------- MBC_UserEvent_Birthday->sendBirthdayEmail END: ' . $this->recipientCount . ' messages sent as Mandrill Send-Template submission - ' . date('D M j G:i:s:u T Y') . ' -------', "\n";
    
    // Reset counter
    $this->recipientCount = 0;
    
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

$status = NULL;

echo '------- mbc-user-event_birthday START: ' . date('D M j G:i:s T Y') . ' -------', "\n";

// Kick off
$mb = new MessageBroker($credentials, $config);
$mb->consumeMessage(array(new MBC_UserEvent_Birthday(), 'consumeBirthdayQueue'));

echo '------- mbp-user-event_birthday END: ' . date('D M j G:i:s T Y') . ' -------', "\n";