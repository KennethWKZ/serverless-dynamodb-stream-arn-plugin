"use strict";

const {
  DynamoDBStreamsClient,
  ListStreamsCommand,
} = require("@aws-sdk/client-dynamodb-streams");

const { fromIni } = require("@aws-sdk/credential-providers");

class fetchDynamoDBStreamsPlugin {
  constructor(serverless, options) {
    this.serverless = serverless;
    this.options = options;

    this.configurationVariablesSources = {
      fetchStreamARN: {
        async resolve({ address, params, resolveConfigurationProperty, __ }) {
          let myStreamArn = null;
          try {
            if (!params || params.length < 1)
              throw new Error(
                "No table name passed to fetchStreamARN Function"
              );

            const configuredRegion = await resolveConfigurationProperty([
              "provider",
              "region",
            ]);

            let region = "";
            if (options && options.region) {
              region = options.region;
            } else if (configuredRegion) {
              region = configuredRegion;
            } else if (params.length > 1) {
              region = params[1];
            }

            const profile = await resolveConfigurationProperty([
              "provider",
              "profile",
            ]);

            const tableName = params[0];
            serverless.cli.log(
              `Fetching Streams of [ Table : ${tableName} ] in region ${region}`
            );

            if (options.localEnvironment) {
              myStreamArn = `arn:aws:dynamodb:${region}:XXXXXXXXXXXX:table/${tableName}/stream/${new Date().toISOString()}`;
              serverless.cli.log(
                `Fetched stream of [ Table : ${tableName} ] => ${myStreamArn}`
              );

              return {
                value: myStreamArn,
              };
            }

            const data = await getDynamoDBStreams({
              region,
              tableName,
              profile,
            });

            myStreamArn = extractStreamARNFromStreamData(data, tableName);
            if (!myStreamArn)
              throw new Error("Could not find stream of this Table");
            else {
              serverless.cli.log(
                `Fetched stream of [ Table : ${tableName} ] => ${myStreamArn}`
              );
            }
          } catch (err) {
            const errorMessage = err && err.message ? err.message : err;
            serverless.cli.log(`Error: ${JSON.stringify(errorMessage)}`);
          }

          return {
            value: myStreamArn,
          };
        },
      },
    };
  }
}

const extractStreamARNFromStreamData = (data, tableName) => {
  const Streams = data.Streams;
  if (!Streams || Streams.length === 0) {
    throw new Error(
      `Cannot Find Stream of [ Table : ${tableName} ], make sure the table exist and stream is enabled`
    );
  }
  const maybeStream = Streams.find((stream) => stream.TableName === tableName);
  if (!maybeStream?.StreamArn) {
    throw new Error(
      `Cannot find stream with table named '${tableName}' among these streams:`,
      Streams
    );
  }
  return maybeStream.StreamArn;
};

const getDynamoDBStreams = async (params) => {
  const clientParams = {
    region: params.region,
  };

  if (params.profile) {
    clientParams.credentials = fromIni({
      profile: params.profile,
    });
  }

  const dynamoStreams = new DynamoDBStreamsClient(clientParams);
  const dynamoStreamsParams = new ListStreamsCommand({
    TableName: params.tableName,
  });
  return dynamoStreams.send(dynamoStreamsParams);
};

module.exports = fetchDynamoDBStreamsPlugin;
