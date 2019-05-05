package rabbeasy

import "github.com/streadway/amqp"

// ExchangesConfig
type exchangesConfig struct {
	logger Logger
}

func (e *exchangesConfig) configureDestinations(channel *amqp.Channel, queueCfg DestinationConfig) (err error) {
	var dlxArgs amqp.Table
	if queueCfg.DlxExchange != "" {
		dlxArgs = amqp.Table{
			"x-dead-letter-exchange":    queueCfg.DlxExchange,
			"x-dead-letter-routing-key": queueCfg.DlxRoutingKey,
		}
		err = e.createExchange(
			channel,
			queueCfg.DlxExchange,
			queueCfg.DlxExchangeType,
			nil,
		)
		if err != nil {
			e.logger.Errorf("Erro ao criar dlx %q %s\n", queueCfg.DlxExchange, err)
			return
		}
	} else {
		dlxArgs = nil
	}
	if queueCfg.Queue != "" {
		_, err = channel.QueueDeclare(
			queueCfg.Queue,
			queueCfg.Durable,
			false,
			false,
			false,
			dlxArgs,
		)
		if err != nil {
			e.logger.Errorf("Erro ao criar fila %s - %s\n", queueCfg.Queue, err)
			return
		}
	}
	if queueCfg.Exchange != "" {
		err = e.createExchange(
			channel,
			queueCfg.Exchange,
			queueCfg.ExchangeType,
			nil,
		)
		if err != nil {
			e.logger.Errorf("Erro ao criar dlx %q %s\n", queueCfg.DlxExchange, err)
			return
		}
	}
	if queueCfg.Queue != "" && queueCfg.Exchange != "" {
		err = channel.QueueBind(
			queueCfg.Queue,
			queueCfg.RoutingKey,
			queueCfg.Exchange,
			false,
			nil,
		)
		if err != nil {
			e.logger.Errorf("Erro ao realizar bind entre o exchange %q e a fila %q - %s\n",
				queueCfg.Exchange, queueCfg.Queue, err)
			return
		}
	}
	return
}

func (e *exchangesConfig) consumeWithDestinations(channel *amqp.Channel, queueCfg DestinationConfig) (messages <-chan amqp.Delivery, err error) {
	messages, err = channel.Consume(
		queueCfg.Queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		e.logger.Errorf("Erro ao criar consumidor para a fila %s - %s\n", queueCfg.Queue, err)
		return
	}
	return
}

func (e *exchangesConfig) createExchange(channel *amqp.Channel, name, kind string, args amqp.Table) (err error) {
	err = channel.ExchangeDeclare(
		name,
		kind,
		true,
		false,
		false,
		false,
		args,
	)
	if err != nil {
		e.logger.Errorf("Erro ao criar exchange %q no rabbitmq - %s\n", name, err)
		return
	}
	return
}
