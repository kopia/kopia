import { findByTestId, render, waitFor } from '@testing-library/react';
import React from 'react';
import { SetupRepository } from '../SetupRepository';
import { setupAPIMock } from './api_mocks';
import { changeControlValue, simulateClick } from './testutils';

it('can create new repository when not initialized', async () => {
  let serverMock = setupAPIMock();

  // first attempt to connect says - NOT_INITIALIZED
  serverMock.onPost('/api/v1/repo/exists', {
    storage: { type: 'filesystem', config: { path: 'some-path' } },
  }).reply(400, {code: 'NOT_INITIALIZED', error: 'repository not initialized'});

  // second attempt to create is success.
  serverMock.onPost('/api/v1/repo/create', {
    storage: { type: 'filesystem', config: { path: 'some-path' } },
    password: 'foo',
    options: {
      blockFormat: { hash: 'h-bar', encryption: 'e-baz' },
      objectFormat: { splitter: 's-foo' }
    },
  }).reply(200, {});

  const { getByTestId, container } = render(<SetupRepository />);

  simulateClick(getByTestId('provider-filesystem'))
  
  changeControlValue(await findByTestId(container, 'control-path'), "some-path")
  simulateClick(getByTestId('submit-button'));

  changeControlValue(await findByTestId(container, 'control-password'), "foo")

  simulateClick(getByTestId('submit-button'));
  await waitFor(() => serverMock.history.post.length == 1);
  changeControlValue(await findByTestId(container, 'control-encryption'), "e-baz")
  changeControlValue(getByTestId("control-splitter"), "s-foo")
  changeControlValue(getByTestId("control-hash"), "h-bar")
  changeControlValue(getByTestId("control-confirmPassword"), "foo")

  simulateClick(getByTestId('submit-button'));
  await waitFor(() => serverMock.history.post.length == 2);
});

it('can connect to existing repository when already initialized', async () => {
  let serverMock = setupAPIMock();

  // first attempt to connect is immediately successful.
  serverMock.onPost('/api/v1/repo/exists', {
    storage: { type: 'filesystem', config: { path: 'some-path' } },
  }).reply(200, {});

  const { getByTestId, container } = render(<SetupRepository />)
  simulateClick(getByTestId('provider-filesystem'));
  changeControlValue(await findByTestId(container, 'control-path'), "some-path")
  simulateClick(getByTestId('submit-button'));
  changeControlValue(await findByTestId(container, 'control-password'), "foo")

  simulateClick(getByTestId('submit-button'));
  await waitFor(() => serverMock.history.post.length == 1);
});

it('can connect to existing repository using token', async () => {
  let serverMock = setupAPIMock();

  serverMock.onPost('/api/v1/repo/connect', {
    token: "my-token",
  }).reply(200, {});

  const { getByTestId, container } = render(<SetupRepository />)
  simulateClick(getByTestId('provider-_token'))
  changeControlValue(await findByTestId(container, 'control-token'), "my-token")

  simulateClick(getByTestId('submit-button'));
  await waitFor(() => serverMock.history.post.length == 1);
});
