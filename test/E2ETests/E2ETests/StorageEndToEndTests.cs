// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Microsoft.Azure.Functions.Tests.E2ETests
{
    
    [Collection(Constants.StorageFunctionAppCollectionName)]
    public class StorageEndToEndTests : IAsyncLifetime
    {
        private readonly FunctionAppFixture _fixture;

        private const int SECONDS = 1000;

        private const int BLOBTIMEOUT = 10 * SECONDS;

        private const int QUEUETIMEOUT = 10 * SECONDS;

        public StorageEndToEndTests(StorageFunctionAppFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public async Task QueueTriggerAndOutput_Succeeds()
        {
            string expectedQueueMessage = Guid.NewGuid().ToString();

            //Trigger
            await StorageHelpers.InsertIntoQueue(Constants.Queue.InputBindingName, expectedQueueMessage);

            //Verify
            var queueMessage = await StorageHelpers.ReadFromQueue(Constants.Queue.OutputBindingName);
            Assert.Equal(expectedQueueMessage, queueMessage);
        }

        [Fact]
        public async Task QueueTriggerAndArrayOutput_Succeeds()
        {
            string expectedQueueMessage = Guid.NewGuid().ToString();

            //Trigger
            await StorageHelpers.InsertIntoQueue(Constants.Queue.InputArrayBindingName, expectedQueueMessage);

            //Verify
            string queueMessage1 = await StorageHelpers.ReadFromQueue(Constants.Queue.OutputArrayBindingName);
            string[] splitMessage1 = queueMessage1.Split("|");
            Assert.Equal(expectedQueueMessage, splitMessage1[0]);
            Assert.True(string.Equals("1", splitMessage1[1]) || string.Equals("2", splitMessage1[1]));

            string queueMessage2 = await StorageHelpers.ReadFromQueue(Constants.Queue.OutputArrayBindingName);
            string[] splitMessage2 = queueMessage2.Split("|");
            Assert.Equal(expectedQueueMessage, splitMessage2[0]);
            Assert.True(string.Equals("1", splitMessage2[1]) || string.Equals("2", splitMessage2[1]));

            Assert.NotEqual(queueMessage1, queueMessage2);
        }

        [Fact]
        public async Task QueueTriggerAndListOutput_Succeeds()
        {
            string expectedQueueMessage = Guid.NewGuid().ToString();

            //Trigger
            await StorageHelpers.InsertIntoQueue(Constants.Queue.InputListBindingName, expectedQueueMessage);

            //Verify
            string queueMessage1 = await StorageHelpers.ReadFromQueue(Constants.Queue.OutputListBindingName);
            string[] splitMessage1 = queueMessage1.Split("|");
            Assert.Equal(expectedQueueMessage, splitMessage1[0]);
            Assert.True(string.Equals("1", splitMessage1[1]) || string.Equals("2", splitMessage1[1]));

            string queueMessage2 = await StorageHelpers.ReadFromQueue(Constants.Queue.OutputListBindingName);
            string[] splitMessage2 = queueMessage2.Split("|");
            Assert.Equal(expectedQueueMessage, splitMessage2[0]);
            Assert.True(string.Equals("1", splitMessage2[1]) || string.Equals("2", splitMessage2[1]));

            Assert.NotEqual(queueMessage1, queueMessage2);
        }

        [Fact]
        public async Task QueueTriggerAndBindingDataOutput_Succeeds()
        {
            string expectedQueueMessage = Guid.NewGuid().ToString();

            //Trigger
            await StorageHelpers.InsertIntoQueue(Constants.Queue.InputBindingDataName, expectedQueueMessage);

            //Verify
            string resultMessage = await StorageHelpers.ReadFromQueue(Constants.Queue.OutputBindingDataName);
            IDictionary<string, string> splitMessage = resultMessage.Split(",").ToDictionary(s => s.Split('=')[0], s => s.Split('=')[1]);

            Assert.Contains("QueueTrigger", splitMessage);
            Assert.Contains("DequeueCount", splitMessage);
            Assert.Contains("Id", splitMessage);
            Assert.Contains("InsertionTime", splitMessage);
            Assert.Contains("NextVisibleTime", splitMessage);
            Assert.Contains("PopReceipt", splitMessage);
        }

        [Fact]
        public async Task QueueTrigger_BindToTriggerMetadata_Succeeds()
        {
            string inputQueueMessage = Guid.NewGuid().ToString();

            //Trigger
            string expectedQueueMessage = await StorageHelpers.InsertIntoQueue(Constants.Queue.InputBindingNameMetadata, inputQueueMessage);

            //Verify
            var queueMessage = await StorageHelpers.ReadFromQueue(Constants.Queue.OutputBindingNameMetadata);
            Assert.Contains(expectedQueueMessage, queueMessage);
        }

        [Fact]
        public async Task QueueTrigger_QueueOutput_Poco_Succeeds()
        {
            string expectedQueueMessage = Guid.NewGuid().ToString();

            //Trigger
            string json = JsonSerializer.Serialize(new { id = expectedQueueMessage });

            await StorageHelpers.InsertIntoQueue(Constants.Queue.InputBindingNamePOCO, json);

            //Verify
            var queueMessage = await StorageHelpers.ReadFromQueue(Constants.Queue.OutputBindingNamePOCO);
            Assert.Contains(expectedQueueMessage, queueMessage);
        }

        [Fact]
        public async Task QueueOutput_PocoList_Succeeds()
        {
            string expectedQueueMessage = Guid.NewGuid().ToString();

            //Trigger
            Assert.True(await HttpHelpers.InvokeHttpTrigger("QueueOutputPocoList", $"?queueMessageId={expectedQueueMessage}", HttpStatusCode.OK, expectedQueueMessage));

            //Verify
            IEnumerable<string> queueMessages = await StorageHelpers.ReadMessagesFromQueue(Constants.Queue.OutputBindingNamePOCO);
            Assert.True(queueMessages.All(msg => msg.Contains(expectedQueueMessage)));
        }

        [Fact]
        public async Task BlobTriggerToBlob_Succeeds()
        {
            string fileName = Guid.NewGuid().ToString();

            //Setup
            await StorageHelpers.UploadFileToContainer(Constants.Blob.InputBindingContainer, fileName);

            //Trigger
            await StorageHelpers.UploadFileToContainer(Constants.Blob.TriggerInputBindingContainer, fileName);

            //Verify
            string result = await StorageHelpers.DownloadFileFromContainer(Constants.Blob.OutputBindingContainer, fileName);

            Assert.Equal("Hello World", result);

            await TestUtility.RetryAsync(() => { 
                var _ = _fixture.TestLogs.CoreToolsLogs.Any(x => x.Contains("Executed 'Functions.BlobTriggerToBlobTest'"));
                return Task.FromResult(_);
            }, 
            timeout: BLOBTIMEOUT, 
            userMessageCallback: () => $"Trigger log was not found"
            );

        }

        [Fact]
        public async Task BlobTriggerPoco_Succeeds()
        {
            string fileName = Guid.NewGuid().ToString();

            //Trigger
            var json = JsonSerializer.Serialize(new { text = "Hello World" });
            await StorageHelpers.UploadFileToContainer(Constants.Blob.TriggerPocoContainer, fileName, json);

            //Verify
            string result = await StorageHelpers.DownloadFileFromContainer(Constants.Blob.OutputPocoContainer, fileName);

            Assert.Equal(json, result);

            await TestUtility.RetryAsync(() => { 
                var _ = _fixture.TestLogs.CoreToolsLogs.Any(x => x.Contains("Executed 'Functions.BlobTriggerPocoTest'"));
                return Task.FromResult(_);
            },
            timeout: BLOBTIMEOUT, 
            userMessageCallback: () => $"Trigger log was not found");
        }

        [Fact]
        public async Task BlobTriggerString_Succeeds()
        {
            string fileName = Guid.NewGuid().ToString();

            //Trigger
            await StorageHelpers.UploadFileToContainer(Constants.Blob.TriggerStringContainer, fileName);

            //Verify
            string result = await StorageHelpers.DownloadFileFromContainer(Constants.Blob.OutputStringContainer, fileName);

            Assert.Equal("Hello World", result);

            await TestUtility.RetryAsync(() => { 
                var _ = _fixture.TestLogs.CoreToolsLogs.Any(x => x.Contains("Executed 'Functions.BlobTriggerStringTest'"));
                return Task.FromResult(_);
            }, 
            timeout: BLOBTIMEOUT, 
            userMessageCallback: () => $"Trigger log was not found"
);
        }

        #region Implementation of IAsyncLifetime

        public async Task InitializeAsync()
        {
            await Task.CompletedTask;
        }

        public async Task DisposeAsync()
        {
            //NOTE: cleanup
            await StorageHelpers.ClearQueues();

            await StorageHelpers.ClearBlobContainers();
        }

        #endregion
    }



    /// <summary>
    /// Class to setup and teardown the storage blobs and queues.
    /// </summary>
    public class StorageFunctionAppFixture : FunctionAppFixture
    {
        
        public StorageFunctionAppFixture(IMessageSink messageSink) : base(messageSink)
        {
        }

        #region Implementation of IAsyncLifetime

        public override async Task InitializeAsync()
        {
            await StorageHelpers.CreateBlobContainers();
            await StorageHelpers.CreateQueues();

            await base.InitializeAsync();
        }

        public override async Task DisposeAsync()
        {
            await base.DisposeAsync();

            //NOTE: Comment this out if you want to keep during local testing.
            await StorageHelpers.DeleteQueues();
            await StorageHelpers.DeleteBlobContainers();
        }

        #endregion
    }

    [CollectionDefinition(Constants.StorageFunctionAppCollectionName)]
    public class StorageFunctionAppCollection : ICollectionFixture<StorageFunctionAppFixture>
    {
        // This class has no code, and is never created. Its purpose is simply
        // to be the place to apply [CollectionDefinition] and all the
        // ICollectionFixture<> interfaces.
    }
}
